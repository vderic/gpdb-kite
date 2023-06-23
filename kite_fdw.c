/*-------------------------------------------------------------------------
 *
 * kite_fdw.c
 *		  Foreign-data wrapper for remote PostgreSQL servers
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/kite_fdw/kite_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "kite_fdw.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "catalog/pg_opfamily.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbgang.h"

#include "kitesdk.h"
#include "nodes/print.h"
#include "xrg.h"
#include "schema.h"
#include "decode.h"
#include "agg.h"
#include "partial_agg.h"


/* source-code-compatibility hacks for pull_varnos() API change */
#define make_restrictinfo(a,b,c,d,e,f,g,h,i) make_restrictinfo_new(a,b,c,d,e,f,g,h,i)

PG_MODULE_MAGIC;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST		0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 * REMINDER: items MUST be a PG node format that is serialized.  Non-serialized
 * object will cause error with error message unrecognized node type: 0 (copyfuncs.c:6874).
 * FdwScanPrivateIndex will pass between segments.
 */
enum FdwScanPrivateIndex
{
	/* Relation Schema (as a String node) */
	FdwScanPrivateSchema,
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Integer representing the desired fetch_size */
	FdwScanPrivateFetchSize,
	/* Integer represeting the number of fragment in kite */
	FdwScanPrivateFragCnt,
	/* FileSpec (serialized kite_filespec_t as a String node) */
	FdwScanPrivateFileSpec,

	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join
	 */
	FdwScanPrivateRelations,

	/* Integer list of aggfnoid retrieved by the SELECT */
	FdwScanPrivateRetrievedAggfnoids,

	/* Integer list of groupby index in Kite by the SELECT */
	FdwScanPrivateRetrievedGroupByAttrs
};

/*
 * Execution state of a foreign scan using kite_fdw.
 */
typedef struct PgFdwScanState
{
	Relation	rel;			/* relcache entry for the foreign table. NULL
								 * for a foreign join scan. */
	TupleDesc	tupdesc;		/* tuple descriptor of scan */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	kite_filespec_t *fspec;                    /* filespec of the table */
	char *schema;				   /* text of schema */
	char *query;				   /* text of SELECT command */
	List *retrieved_attrs;		   /* list of retrieved attribute numbers */
	List *retrieved_groupby_attrs; /* list of retrieved group by index in kite */
	List *retrieved_aggfnoids;	 /* list of retrieved aggfnoids from target list */

	/* for remote query execution */
	xrg_agg_t *agg;		 /* xrg_agg_t for aggregate */
	xrg_agg_p_t *partial_agg; /* xrg_agg_p_t for partial aggregate */
	kite_request_t *req; /* kite connectino for the scan */
	bool mpp_allsegment; /* mpp allsegment */
	int fragid; /* fragment id */
	int fragcnt; /* number of fragment in kite */

	/* for remote query execution */
	unsigned int cursor_number; /* quasi-unique ID for my cursor */
	bool		cursor_exists;	/* have we created the cursor? */
	int			numParams;		/* number of parameters passed to query */
	FmgrInfo   *param_flinfo;	/* output conversion functions for them */
	List	   *param_exprs;	/* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */

	/* for storing result tuples */
	HeapTuple  *tuples;			/* array of currently-retrieved tuples */
	int			num_tuples;		/* # of tuples in array */
	int			next_tuple;		/* index of next one to return */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	int			fetch_ct_2;		/* Min(# of fetches done, 2) */
	bool		eof_reached;	/* true if last fetch reached EOF */

	/* working memory contexts */
	MemoryContext batch_cxt;	/* context holding current batch of tuples */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */

	int			fetch_size;		/* number of tuples per fetch */
} PgFdwScanState;

/*
 * Workspace for analyzing a foreign table.
 */
typedef struct PgFdwAnalyzeState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */
	List	   *retrieved_attrs;	/* attr numbers retrieved by query */

	/* collected sample rows */
	HeapTuple  *rows;			/* array of size targrows */
	int			targrows;		/* target # of sample rows */
	int			numrows;		/* # of sample rows collected */

	/* for random sampling */
	double		samplerows;		/* # of rows fetched */
	double		rowstoskip;		/* # of rows to skip before next sample */
	ReservoirStateData rstate;	/* state for reservoir sampling */

	/* working memory contexts */
	MemoryContext anl_cxt;		/* context for per-analyze lifespan data */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} PgFdwAnalyzeState;

/*
 * This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex
{
	/* has-final-sort flag (as an integer Value node) */
	FdwPathPrivateHasFinalSort,
	/* has-limit flag (as an integer Value node) */
	FdwPathPrivateHasLimit
};

/* Struct for extra information passed to estimate_path_cost_size() */
typedef struct
{
	PathTarget *target;
	bool		has_final_sort;
	bool		has_limit;
	double		limit_tuples;
	int64		count_est;
	int64		offset_est;
} PgFdwPathExtraData;

/*
 * Identify the attribute where data conversion fails.
 */
typedef struct ConversionLocation
{
	AttrNumber	cur_attno;		/* attribute number being processed, or 0 */
	Relation	rel;			/* foreign table being processed, or NULL */
	ForeignScanState *fsstate;	/* plan node being processed, or NULL */
} ConversionLocation;

/* Callback argument for ec_member_matches_foreign */
typedef struct
{
	Expr	   *current;		/* current expr, or NULL if not yet found */
	List	   *already_used;	/* expressions already dealt with */
} ec_member_foreign_arg;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(kite_fdw_handler);

/*
 * FDW callback routines
 */
static void kiteGetForeignRelSize(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreigntableid);
static void kiteGetForeignPaths(PlannerInfo *root,
									RelOptInfo *baserel,
									Oid foreigntableid);
static ForeignScan *kiteGetForeignPlan(PlannerInfo *root,
										   RelOptInfo *foreignrel,
										   Oid foreigntableid,
										   ForeignPath *best_path,
										   List *tlist,
										   List *scan_clauses,
										   Plan *outer_plan);
static void kiteBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *kiteIterateForeignScan(ForeignScanState *node);
static void kiteReScanForeignScan(ForeignScanState *node);
static void kiteEndForeignScan(ForeignScanState *node);
static void kiteExplainForeignScan(ForeignScanState *node,
									   ExplainState *es);
static bool kiteAnalyzeForeignTable(Relation relation,
										AcquireSampleRowsFunc *func,
										BlockNumber *totalpages);
static void kiteGetForeignUpperPaths(PlannerInfo *root,
										 UpperRelationKind stage,
										 RelOptInfo *input_rel,
										 RelOptInfo *output_rel,
										 void *extra);
/*
 * Helper functions
 */
static void estimate_path_cost_size(PlannerInfo *root,
									RelOptInfo *foreignrel,
									List *param_join_conds,
									List *pathkeys,
									PgFdwPathExtraData *fpextra,
									double *p_rows, int *p_width,
									Cost *p_startup_cost, Cost *p_total_cost);
static void adjust_foreign_grouping_path_cost(PlannerInfo *root,
											  List *pathkeys,
											  double retrieved_rows,
											  double width,
											  double limit_tuples,
											  Cost *p_startup_cost,
											  Cost *p_run_cost);
static bool ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
									  EquivalenceClass *ec, EquivalenceMember *em,
									  void *arg);
static void create_cursor(ForeignScanState *node);
static void fetch_more_data(ForeignScanState *node);
static void process_query_params(ExprContext *econtext,
								 FmgrInfo *param_flinfo,
								 List *param_exprs,
								 const char **param_values);
static int	kiteAcquireSampleRowsFunc(Relation relation, int elevel,
										  HeapTuple *rows, int targrows,
										  double *totalrows,
										  double *totaldeadrows);
/* KITE */
static HeapTuple make_tuple_from_agg(xrg_agg_t *agg,
	int row,
	Relation rel,
	AttInMetadata *attinmeta,
	List *retrieved_attrs,
	ForeignScanState *fsstate,
	MemoryContext temp_context);

static HeapTuple make_tuple_from_partial_agg(xrg_agg_p_t *agg,
        xrg_iter_t *iter,
        int row,
        Relation rel,
        AttInMetadata *attinmeta,
        List *retrieved_attrs,
        ForeignScanState *fsstate,
        MemoryContext temp_context);

static HeapTuple make_tuple_from_result_row(xrg_iter_t *iter,
	int row,
	Relation rel,
	AttInMetadata *attinmeta,
	List *retrieved_attrs,
	ForeignScanState *fsstate,
	MemoryContext temp_context);

static bool kite_get_relation_stats(PgFdwRelationInfo *fpinfo, Relation relation, kite_filespec_t *fspec,
                BlockNumber *totalpages, double *totalrows);

/* END KITE */

static void conversion_error_callback(void *arg);
static bool foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
								Node *havingQual);
static List *get_useful_pathkeys_for_relation(PlannerInfo *root,
											  RelOptInfo *rel);
static List *get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel);
static void add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
											Path *epq_path);
static void add_foreign_grouping_paths(PlannerInfo *root,
									   RelOptInfo *input_rel,
									   RelOptInfo *grouped_rel,
									   GroupPathExtraData *extra);
static void add_foreign_final_paths(PlannerInfo *root,
									RelOptInfo *input_rel,
									RelOptInfo *final_rel,
									FinalPathExtraData *extra);
static void apply_server_options(PgFdwRelationInfo *fpinfo);
static void apply_table_options(PgFdwRelationInfo *fpinfo);
static void merge_fdw_options(PgFdwRelationInfo *fpinfo,
							  const PgFdwRelationInfo *fpinfo_o,
							  const PgFdwRelationInfo *fpinfo_i);


/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
kite_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = kiteGetForeignRelSize;
	routine->GetForeignPaths = kiteGetForeignPaths;
	routine->GetForeignPlan = kiteGetForeignPlan;
	routine->BeginForeignScan = kiteBeginForeignScan;
	routine->IterateForeignScan = kiteIterateForeignScan;
	routine->ReScanForeignScan = kiteReScanForeignScan;
	routine->EndForeignScan = kiteEndForeignScan;

	/* Function for EvalPlanQual rechecks */
	// routine->RecheckForeignScan = kiteRecheckForeignScan;
	
	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = kiteExplainForeignScan;

	/* Support functions for ANALYZE */
	routine->AnalyzeForeignTable = kiteAnalyzeForeignTable;

	/* Support functions for upper relation push-down */
	routine->GetForeignUpperPaths = kiteGetForeignUpperPaths;

	PG_RETURN_POINTER(routine);
}

/*
 * kiteGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
static void
kiteGetForeignRelSize(PlannerInfo *root,
						  RelOptInfo *baserel,
						  Oid foreigntableid)
{
	PgFdwRelationInfo *fpinfo;
	ListCell   *lc;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	const char *namespace;
	const char *relname;
	const char *refname;

	/*
	 * We use PgFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = (PgFdwRelationInfo *) palloc0(sizeof(PgFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	/*
	 * Extract user-settable option values.  Note that per-table settings of
	 * use_remote_estimate and fetch_size override per-server settings of
	 * them, respectively.
	 */
	fpinfo->use_remote_estimate = false;
	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
	fpinfo->shippable_extensions = NIL;
	fpinfo->fetch_size = 100;
	fpinfo->fragcnt = -1;
	fpinfo->csv_delim = ',';
	fpinfo->csv_quote = '"';
	fpinfo->csv_escape = '"';
	fpinfo->csv_nullstr = 0;
	fpinfo->csv_header = false;

	apply_server_options(fpinfo);
	apply_table_options(fpinfo);

	/*
	 * If the table or the server is configured to use remote estimates,
	 * identify which user to do remote access as during planning.  This
	 * should match what ExecCheckRTEPerms() does.  If we fail due to lack of
	 * permissions, the query would have failed at runtime anyway.
	 */
	if (fpinfo->use_remote_estimate)
	{
		Oid			userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

		fpinfo->user = GetUserMapping(userid, fpinfo->server->serverid);
	}
	else
		fpinfo->user = NULL;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	classifyConditions(root, baserel, baserel->baserestrictinfo,
					   &fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 baserel->relid,
													 JOIN_INNER,
													 NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction clauses, as well as the
	 * average row width.  Otherwise, estimate using whatever statistics we
	 * have locally, in a way similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		/*
		 * Get cost/size estimates with help of remote server.  Save the
		 * values in fpinfo so we don't need to do it again to generate the
		 * basic foreign path.
		 */
		estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
								&fpinfo->rows, &fpinfo->width,
								&fpinfo->startup_cost, &fpinfo->total_cost);

		/* Report estimated baserel size to planner. */
		baserel->rows = fpinfo->rows;
		baserel->reltarget->width = fpinfo->width;
	}
	else
	{
		/*
		 * If the foreign table has never been ANALYZEd, it will have relpages
		 * and reltuples equal to zero, which most likely has nothing to do
		 * with reality.  We can't do a whole lot about that if we're not
		 * allowed to consult the remote server, but we can use a hack similar
		 * to plancat.c's treatment of empty relations: use a minimum size
		 * estimate of 10 pages, and divide by the column-datatype-based width
		 * estimate to get the corresponding number of tuples.
		 */
		if (baserel->pages == 0 && baserel->tuples == 0)
		{
			baserel->pages = 10;
			baserel->tuples =
				(10 * BLCKSZ) / (baserel->reltarget->width +
								 MAXALIGN(SizeofHeapTupleHeader));
		}

		/* Estimate baserel size as best we can with local statistics. */
		set_baserel_size_estimates(root, baserel);

		/* Fill in basically-bogus cost estimates for use later. */
		estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
								&fpinfo->rows, &fpinfo->width,
								&fpinfo->startup_cost, &fpinfo->total_cost);
	}

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output. We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */
	fpinfo->relation_name = makeStringInfo();
	namespace = get_namespace_name(get_rel_namespace(foreigntableid));
	relname = get_rel_name(foreigntableid);
	refname = rte->eref->aliasname;
	appendStringInfo(fpinfo->relation_name, "%s.%s",
					 quote_identifier(namespace),
					 quote_identifier(relname));
	if (*refname && strcmp(refname, relname) != 0)
		appendStringInfo(fpinfo->relation_name, " %s",
						 quote_identifier(rte->eref->aliasname));

	/* No outer and inner relations. */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	fpinfo->lower_subquery_rels = NULL;
	/* Set the relation index. */
	fpinfo->relation_index = baserel->relid;
}

/*
 * get_useful_ecs_for_relation
 *		Determine which EquivalenceClasses might be involved in useful
 *		orderings of this relation.
 *
 * This function is in some respects a mirror image of the core function
 * pathkeys_useful_for_merging: for a regular table, we know what indexes
 * we have and want to test whether any of them are useful.  For a foreign
 * table, we don't know what indexes are present on the remote side but
 * want to speculate about which ones we'd like to use if they existed.
 *
 * This function returns a list of potentially-useful equivalence classes,
 * but it does not guarantee that an EquivalenceMember exists which contains
 * Vars only from the given relation.  For example, given ft1 JOIN t1 ON
 * ft1.x + t1.x = 0, this function will say that the equivalence class
 * containing ft1.x + t1.x is potentially useful.  Supposing ft1 is remote and
 * t1 is local (or on a different server), it will turn out that no useful
 * ORDER BY clause can be generated.  It's not our job to figure that out
 * here; we're only interested in identifying relevant ECs.
 */
static List *
get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_eclass_list = NIL;
	ListCell   *lc;
	Relids		relids;

	/*
	 * First, consider whether any active EC is potentially useful for a merge
	 * join against this relation.
	 */
	if (rel->has_eclass_joins)
	{
		foreach(lc, root->eq_classes)
		{
			EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);

			if (eclass_useful_for_merging(root, cur_ec, rel))
				useful_eclass_list = lappend(useful_eclass_list, cur_ec);
		}
	}

	/*
	 * Next, consider whether there are any non-EC derivable join clauses that
	 * are merge-joinable.  If the joininfo list is empty, we can exit
	 * quickly.
	 */
	if (rel->joininfo == NIL)
		return useful_eclass_list;

	/* If this is a child rel, we must use the topmost parent rel to search. */
	if (IS_OTHER_REL(rel))
	{
		Assert(!bms_is_empty(rel->top_parent_relids));
		relids = rel->top_parent_relids;
	}
	else
		relids = rel->relids;

	/* Check each join clause in turn. */
	foreach(lc, rel->joininfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/* Consider only mergejoinable clauses */
		if (restrictinfo->mergeopfamilies == NIL)
			continue;

		/* Make sure we've got canonical ECs. */
		update_mergeclause_eclasses(root, restrictinfo);

		/*
		 * restrictinfo->mergeopfamilies != NIL is sufficient to guarantee
		 * that left_ec and right_ec will be initialized, per comments in
		 * distribute_qual_to_rels.
		 *
		 * We want to identify which side of this merge-joinable clause
		 * contains columns from the relation produced by this RelOptInfo. We
		 * test for overlap, not containment, because there could be extra
		 * relations on either side.  For example, suppose we've got something
		 * like ((A JOIN B ON A.x = B.x) JOIN C ON A.y = C.y) LEFT JOIN D ON
		 * A.y = D.y.  The input rel might be the joinrel between A and B, and
		 * we'll consider the join clause A.y = D.y. relids contains a
		 * relation not involved in the join class (B) and the equivalence
		 * class for the left-hand side of the clause contains a relation not
		 * involved in the input rel (C).  Despite the fact that we have only
		 * overlap and not containment in either direction, A.y is potentially
		 * useful as a sort column.
		 *
		 * Note that it's even possible that relids overlaps neither side of
		 * the join clause.  For example, consider A LEFT JOIN B ON A.x = B.x
		 * AND A.x = 1.  The clause A.x = 1 will appear in B's joininfo list,
		 * but overlaps neither side of B.  In that case, we just skip this
		 * join clause, since it doesn't suggest a useful sort order for this
		 * relation.
		 */
		if (bms_overlap(relids, restrictinfo->right_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->right_ec);
		else if (bms_overlap(relids, restrictinfo->left_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->left_ec);
	}

	return useful_eclass_list;
}

/*
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_pathkeys_list = NIL;
	List	   *useful_eclass_list;
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) rel->fdw_private;
	EquivalenceClass *query_ec = NULL;
	ListCell   *lc;

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	fpinfo->qp_is_pushdown_safe = false;
	if (root->query_pathkeys)
	{
		bool		query_pathkeys_ok = true;

		foreach(lc, root->query_pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(lc);

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 */
			if (!is_foreign_pathkey(root, rel, pathkey))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
		{
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
			fpinfo->qp_is_pushdown_safe = true;
		}
	}

	/*
	 * Even if we're not using remote estimates, having the remote side do the
	 * sort generally won't be any worse than doing it locally, and it might
	 * be much better if the remote side can generate data in the right order
	 * without needing a sort at all.  However, what we're going to do next is
	 * try to generate pathkeys that seem promising for possible merge joins,
	 * and that's more speculative.  A wrong choice might hurt quite a bit, so
	 * bail out if we can't use remote estimates.
	 */
	if (!fpinfo->use_remote_estimate)
		return useful_pathkeys_list;

	/* Get the list of interesting EquivalenceClasses. */
	useful_eclass_list = get_useful_ecs_for_relation(root, rel);

	/* Extract unique EC for query, if any, so we don't consider it again. */
	if (list_length(root->query_pathkeys) == 1)
	{
		PathKey    *query_pathkey = linitial(root->query_pathkeys);

		query_ec = query_pathkey->pk_eclass;
	}

	/*
	 * As a heuristic, the only pathkeys we consider here are those of length
	 * one.  It's surely possible to consider more, but since each one we
	 * choose to consider will generate a round-trip to the remote side, we
	 * need to be a bit cautious here.  It would sure be nice to have a local
	 * cache of information about remote index definitions...
	 */
	foreach(lc, useful_eclass_list)
	{
		EquivalenceClass *cur_ec = lfirst(lc);
		PathKey    *pathkey;

		/* If redundant with what we did above, skip it. */
		if (cur_ec == query_ec)
			continue;

		/* Can't push down the sort if the EC's opfamily is not shippable. */
		if (!is_shippable(linitial_oid(cur_ec->ec_opfamilies),
						  OperatorFamilyRelationId, fpinfo))
			continue;

		/* If no pushable expression for this rel, skip it. */
		if (find_em_for_rel(root, cur_ec, rel) == NULL)
			continue;

		/* Looks like we can generate a pathkey, so let's do it. */
		pathkey = make_canonical_pathkey(root, cur_ec,
										 linitial_oid(cur_ec->ec_opfamilies),
										 BTLessStrategyNumber,
										 false);
		useful_pathkeys_list = lappend(useful_pathkeys_list,
									   list_make1(pathkey));
	}

	return useful_pathkeys_list;
}

/*
 * kiteGetForeignPaths
 *		Create possible scan paths for a scan on the foreign table
 */
static void
kiteGetForeignPaths(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid)
{
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) baserel->fdw_private;
	ForeignPath *path;
	List	   *ppi_list;
	ListCell   *lc;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 *
	 * Although this path uses no join clauses, it could still have required
	 * parameterization due to LATERAL refs in its tlist.
	 */
	path = create_foreignscan_path(root, baserel,
								   NULL,	/* default pathtarget */
								   fpinfo->rows,
								   fpinfo->startup_cost,
								   fpinfo->total_cost,
								   NIL, /* no pathkeys */
								   baserel->lateral_relids,
								   NULL,	/* no extra plan */
								   NIL);	/* no fdw_private list */
	add_path(baserel, (Path *) path);

	/* KITE do not support ORDER BY */
#if 0
	/* Add paths with pathkeys */
	add_paths_with_pathkeys_for_rel(root, baserel, NULL);
#endif

	/*
	 * If we're not using remote estimates, stop here.  We have no way to
	 * estimate whether any join clauses would be worth sending across, so
	 * don't bother building parameterized paths.
	 */
	if (!fpinfo->use_remote_estimate)
		return;

	/*
	 * Thumb through all join clauses for the rel to identify which outer
	 * relations could supply one or more safe-to-send-to-remote join clauses.
	 * We'll build a parameterized path for each such outer relation.
	 *
	 * It's convenient to manage this by representing each candidate outer
	 * relation by the ParamPathInfo node for it.  We can then use the
	 * ppi_clauses list in the ParamPathInfo node directly as a list of the
	 * interesting join clauses for that rel.  This takes care of the
	 * possibility that there are multiple safe join clauses for such a rel,
	 * and also ensures that we account for unsafe join clauses that we'll
	 * still have to enforce locally (since the parameterized-path machinery
	 * insists that we handle all movable clauses).
	 */
	ppi_list = NIL;
	foreach(lc, baserel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Relids		required_outer;
		ParamPathInfo *param_info;

		/* Check if clause can be moved to this rel */
		if (!join_clause_is_movable_to(rinfo, baserel))
			continue;

		/* See if it is safe to send to remote */
		if (!is_foreign_expr(root, baserel, rinfo->clause))
			continue;

		/* Calculate required outer rels for the resulting path */
		required_outer = bms_union(rinfo->clause_relids,
								   baserel->lateral_relids);
		/* We do not want the foreign rel itself listed in required_outer */
		required_outer = bms_del_member(required_outer, baserel->relid);

		/*
		 * required_outer probably can't be empty here, but if it were, we
		 * couldn't make a parameterized path.
		 */
		if (bms_is_empty(required_outer))
			continue;

		/* Get the ParamPathInfo */
		param_info = get_baserel_parampathinfo(root, baserel,
											   required_outer);
		Assert(param_info != NULL);

		/*
		 * Add it to list unless we already have it.  Testing pointer equality
		 * is OK since get_baserel_parampathinfo won't make duplicates.
		 */
		ppi_list = list_append_unique_ptr(ppi_list, param_info);
	}

	/*
	 * The above scan examined only "generic" join clauses, not those that
	 * were absorbed into EquivalenceClauses.  See if we can make anything out
	 * of EquivalenceClauses.
	 */
	if (baserel->has_eclass_joins)
	{
		/*
		 * We repeatedly scan the eclass list looking for column references
		 * (or expressions) belonging to the foreign rel.  Each time we find
		 * one, we generate a list of equivalence joinclauses for it, and then
		 * see if any are safe to send to the remote.  Repeat till there are
		 * no more candidate EC members.
		 */
		ec_member_foreign_arg arg;

		arg.already_used = NIL;
		for (;;)
		{
			List	   *clauses;

			/* Make clauses, skipping any that join to lateral_referencers */
			arg.current = NULL;
			clauses = generate_implied_equalities_for_column(root,
															 baserel,
															 ec_member_matches_foreign,
															 (void *) &arg,
															 baserel->lateral_referencers);

			/* Done if there are no more expressions in the foreign rel */
			if (arg.current == NULL)
			{
				Assert(clauses == NIL);
				break;
			}

			/* Scan the extracted join clauses */
			foreach(lc, clauses)
			{
				RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
				Relids		required_outer;
				ParamPathInfo *param_info;

				/* Check if clause can be moved to this rel */
				if (!join_clause_is_movable_to(rinfo, baserel))
					continue;

				/* See if it is safe to send to remote */
				if (!is_foreign_expr(root, baserel, rinfo->clause))
					continue;

				/* Calculate required outer rels for the resulting path */
				required_outer = bms_union(rinfo->clause_relids,
										   baserel->lateral_relids);
				required_outer = bms_del_member(required_outer, baserel->relid);
				if (bms_is_empty(required_outer))
					continue;

				/* Get the ParamPathInfo */
				param_info = get_baserel_parampathinfo(root, baserel,
													   required_outer);
				Assert(param_info != NULL);

				/* Add it to list unless we already have it */
				ppi_list = list_append_unique_ptr(ppi_list, param_info);
			}

			/* Try again, now ignoring the expression we found this time */
			arg.already_used = lappend(arg.already_used, arg.current);
		}
	}

	/*
	 * Now build a path for each useful outer relation.
	 */
	foreach(lc, ppi_list)
	{
		ParamPathInfo *param_info = (ParamPathInfo *) lfirst(lc);
		double		rows;
		int			width;
		Cost		startup_cost;
		Cost		total_cost;

		/* Get a cost estimate from the remote */
		estimate_path_cost_size(root, baserel,
								param_info->ppi_clauses, NIL, NULL,
								&rows, &width,
								&startup_cost, &total_cost);

		/*
		 * ppi_rows currently won't get looked at by anything, but still we
		 * may as well ensure that it matches our idea of the rowcount.
		 */
		param_info->ppi_rows = rows;

		/* Make the path */
		path = create_foreignscan_path(root, baserel,
									   NULL,	/* default pathtarget */
									   rows,
									   startup_cost,
									   total_cost,
									   NIL, /* no pathkeys */
									   param_info->ppi_req_outer,
									   NULL,
									   NIL);	/* no fdw_private list */
		add_path(baserel, (Path *) path);
	}
}

/*
 * kiteGetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
static ForeignScan *
kiteGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *foreignrel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses,
					   Plan *outer_plan)
{
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;
	Index		scan_relid;
	List	   *fdw_private;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *fdw_scan_tlist = NIL;
	List	   *fdw_recheck_quals = NIL;
	List	   *retrieved_attrs;
	List *retrieved_aggfnoids = NIL;
	List *retrieved_groupby_attrs = NIL;
	StringInfoData sql;
	StringInfoData schema;
	bool		has_final_sort = false;
	bool		has_limit = false;
	ListCell   *lc;
	kite_filespec_t *fspec;
	StringInfoData fspecstr;

	/*
	 * Get FDW private data created by kiteGetForeignUpperPaths(), if any.
	 */
	if (best_path->fdw_private)
	{
		has_final_sort = intVal(list_nth(best_path->fdw_private,
										 FdwPathPrivateHasFinalSort));
		has_limit = intVal(list_nth(best_path->fdw_private,
									FdwPathPrivateHasLimit));
	}

	if (IS_SIMPLE_REL(foreignrel))
	{
		/*
		 * For base relations, set scan_relid as the relid of the relation.
		 */
		scan_relid = foreignrel->relid;

		/*
		 * In a base-relation scan, we must apply the given scan_clauses.
		 *
		 * Separate the scan_clauses into those that can be executed remotely
		 * and those that can't.  baserestrictinfo clauses that were
		 * previously determined to be safe or unsafe by classifyConditions
		 * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
		 * else in the scan_clauses list will be a join clause, which we have
		 * to check for remote-safety.
		 *
		 * Note: the join clauses we see here should be the exact same ones
		 * previously examined by kiteGetForeignPaths.  Possibly it'd be
		 * worth passing forward the classification work done then, rather
		 * than repeating it here.
		 *
		 * This code must match "extract_actual_clauses(scan_clauses, false)"
		 * except for the additional decision about remote versus local
		 * execution.
		 */
		foreach(lc, scan_clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			/* Ignore any pseudoconstants, they're dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (is_foreign_expr(root, foreignrel, rinfo->clause))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
		fdw_recheck_quals = remote_exprs;
	}
	else
	{
		/*
		 * Join relation or upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see kiteGetForeignJoinPaths().
		 * If we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = build_tlist_to_deparse(foreignrel);

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot.  Also, remove the local conditions
		 * from outer plan's quals, lest they be evaluated twice, once by the
		 * local plan and once by the scan.
		 */
		if (outer_plan)
		{
			ListCell   *lc;

			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins. Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(foreignrel));

			/*
			 * First, update the plan's qual list if possible.  In some cases
			 * the quals might be enforced below the topmost plan level, in
			 * which case we'll fail to remove them; it's not worth working
			 * harder than this.
			 */
			foreach(lc, local_exprs)
			{
				Node	   *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.  (They might also be
				 * in the mergequals or hashquals, but we can't touch those
				 * without breaking the plan.)
				 */
				if (IsA(outer_plan, NestLoop) ||
					IsA(outer_plan, MergeJoin) ||
					IsA(outer_plan, HashJoin))
				{
					Join	   *join_plan = (Join *) outer_plan;

					if (join_plan->jointype == JOIN_INNER)
						join_plan->joinqual = list_delete(join_plan->joinqual,
														  qual);
				}
			}

			/*
			 * Now fix the subplan's tlist --- this might result in inserting
			 * a Result node atop the plan tree.
			 */
			outer_plan = change_plan_targetlist(outer_plan, fdw_scan_tlist,
												best_path->path.parallel_safe);
		}
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
							remote_exprs, best_path->path.pathkeys,
							has_final_sort, has_limit, false,
							&retrieved_attrs, &params_list,
							&retrieved_aggfnoids, &retrieved_groupby_attrs);

	/* Remember remote_exprs for possible use by kitePlanDirectModify */
	fpinfo->final_remote_exprs = remote_exprs;

	/*
	 * build the schema for KITE
	 */
	{
		RelOptInfo *relinfo = IS_UPPER_REL(foreignrel) ? fpinfo->outerrel : foreignrel;
		RangeTblEntry *scanrte = planner_rt_fetch(relinfo->relid, root);
		Relation scanrel = table_open(scanrte->relid, NoLock);
		TupleDesc tupdesc = RelationGetDescr(scanrel);
		initStringInfo(&schema);
		kite_build_schema(&schema, tupdesc);
		table_close(scanrel, NoLock);
	}

	/*
	 * build filespec for KITE
	 */
	fspec = (kite_filespec_t*) palloc0(sizeof(kite_filespec_t));
	if (! fpinfo->fmt) {
		elog(ERROR, "table option fmt is not specified");
	}

	strcpy(fspec->fmt, fpinfo->fmt);
	fspec->u.csv.delim = fpinfo->csv_delim;
	fspec->u.csv.quote = fpinfo->csv_quote;
	fspec->u.csv.escape = fpinfo->csv_escape;
	if (fpinfo->csv_nullstr) {
		strcpy(fspec->u.csv.nullstr, fpinfo->csv_nullstr);
	} else {
		*fspec->u.csv.nullstr = 0;
	}
	fspec->u.csv.header_line = fpinfo->csv_header;

	initStringInfo(&fspecstr);
	filespec_serialize(fspec, &fspecstr);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match order in enum FdwScanPrivateIndex.
	 */
	fdw_private = list_make5(makeString(schema.data),
					makeString(sql.data),
					retrieved_attrs,
					makeInteger(fpinfo->fetch_size),
					makeInteger(fpinfo->fragcnt));

	fdw_private = lappend(fdw_private, makeString(fspecstr.data));

	if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel)) {
		fdw_private = lappend(fdw_private,
							  makeString(fpinfo->relation_name->data));

		fdw_private = lappend(fdw_private, retrieved_aggfnoids);

		if (retrieved_groupby_attrs) {
			fdw_private = lappend(fdw_private, retrieved_groupby_attrs);
		}

	}


	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							params_list,
							fdw_private,
							fdw_scan_tlist,
							fdw_recheck_quals,
							outer_plan);
}

/*
 * kiteBeginForeignScan
 *		Initiate an executor scan of a foreign PostgreSQL table.
 */
static void
kiteBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	PgFdwScanState *fsstate;
	RangeTblEntry *rte;
	Oid			userid;
	ForeignTable *table;
	UserMapping *user;
	int			rtindex;
	int			numParams;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	fsstate = (PgFdwScanState *) palloc0(sizeof(PgFdwScanState));
	node->fdw_state = (void *) fsstate;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
	 * lowest-numbered member RTE as a representative; we would get the same
	 * result from any.
	 */
	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
	rte = exec_rt_fetch(rtindex, estate);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(rte->relid);
	user = GetUserMapping(userid, table->serverid);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fsstate->req = GetConnection(user, false);

	/* Assign a unique ID for my cursor */
	fsstate->cursor_number = 0;
	fsstate->cursor_exists = false;

	/* Get private info created by planner functions. */
	fsstate->schema = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateSchema));
	fsstate->query = strVal(list_nth(fsplan->fdw_private,
									 FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
												 FdwScanPrivateRetrievedAttrs);
	fsstate->fetch_size = intVal(list_nth(fsplan->fdw_private,
										  FdwScanPrivateFetchSize));
	fsstate->fragcnt = intVal(list_nth(fsplan->fdw_private,
		FdwScanPrivateFragCnt));
	char *fspec = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateFileSpec));
	fsstate->fspec = filespec_deserialize(fspec);

	/* Create contexts for batches of tuples and per-tuple temp workspace. */
	fsstate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
											   "kite_fdw tuple data",
											   ALLOCSET_DEFAULT_SIZES);
	fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "kite_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	if (list_length(fsplan->fdw_private) >= FdwScanPrivateRetrievedAggfnoids + 1) {
		fsstate->retrieved_aggfnoids = (List *)list_nth(fsplan->fdw_private,
			FdwScanPrivateRetrievedAggfnoids);
	}

	if (list_length(fsplan->fdw_private) >= FdwScanPrivateRetrievedGroupByAttrs + 1) {
		fsstate->retrieved_groupby_attrs = (List *)list_nth(fsplan->fdw_private,
			FdwScanPrivateRetrievedGroupByAttrs);
	}

	if (table->exec_location == FTEXECLOCATION_MULTI_SERVERS && Gp_role == GP_ROLE_EXECUTE) {
		// parallel load here
		fsstate->fragid = GpIdentity.segindex;

		if (fsstate->fragcnt < 0) {
			fsstate->fragcnt = getgpsegmentCount();
		}
	       	if (fsstate->fragcnt > getgpsegmentCount()) {
			fsstate->fragcnt = getgpsegmentCount();
		}

		fsstate->mpp_allsegment = true;
	} else {
		// single server or all segments
		fsstate->mpp_allsegment = false;
		fsstate->fragid = -1;
		if (fsstate->fragcnt < 0) {
			//elog(INFO, "fragcnt cannot be -1 when mpp_execute = 'master|any'. set fragcnt = 1.");
			fsstate->fragcnt = 1;
		}
	}

	/*
	 * Get info we'll need for converting data fetched from the foreign server
	 * into local representation and error reporting during that process.
	 */
	if (fsplan->scan.scanrelid > 0)
	{
		fsstate->rel = node->ss.ss_currentRelation;
		fsstate->tupdesc = RelationGetDescr(fsstate->rel);
	}
	else
	{
		fsstate->rel = NULL;
		fsstate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	fsstate->attinmeta = TupleDescGetAttInMetadata(fsstate->tupdesc);

	/*
	 * Prepare for processing of parameters used in remote query, if any.
	 */
	numParams = list_length(fsplan->fdw_exprs);
	fsstate->numParams = numParams;

#if 1 
	/* KITE */
	if (numParams > 0) {
		elog(ERROR, "Statement with muiltple parameters not supported");
	}
	/* END KITE */
#else
	if (numParams > 0)
		prepare_query_params((PlanState *) node,
							 fsplan->fdw_exprs,
							 numParams,
							 &fsstate->param_flinfo,
							 &fsstate->param_exprs,
							 &fsstate->param_values);
#endif
	/*
	 * Prepare xrg_agg
	 */
	fsstate->agg = NULL;
	fsstate->partial_agg = NULL;
	if (fsstate->retrieved_aggfnoids) {
		if (fsstate->mpp_allsegment) {
			fsstate->partial_agg = xrg_agg_p_init(fsstate->retrieved_attrs,
				fsstate->retrieved_aggfnoids,
				fsstate->retrieved_groupby_attrs);

		} else {
			fsstate->agg = xrg_agg_init(fsstate->retrieved_attrs,
				fsstate->retrieved_aggfnoids,
				fsstate->retrieved_groupby_attrs);
		}
	}
}

/*
 * kiteIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
static TupleTableSlot *
kiteIterateForeignScan(ForeignScanState *node)
{
	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/*
	 * If this is the first call after Begin or ReScan, we need to create the
	 * cursor on the remote side.
	 */
	if (!fsstate->cursor_exists)
		create_cursor(node);

	/*
	 * Get some more tuples, if we've run out.
	 */
	if (fsstate->next_tuple >= fsstate->num_tuples)
	{
		/* No point in another fetch if we already detected EOF, though. */
		if (!fsstate->eof_reached)
			fetch_more_data(node);
		/* If we didn't get any tuples, must be end of data. */
		if (fsstate->next_tuple >= fsstate->num_tuples)
			return ExecClearTuple(slot);
	}

	/*
	 * Return the next tuple.
	 */
	ExecStoreHeapTuple(fsstate->tuples[fsstate->next_tuple++],
					   slot,
					   false);

	return slot;
}

/*
 * kiteReScanForeignScan
 *		Restart the scan.
 */
static void
kiteReScanForeignScan(ForeignScanState *node)
{
	/* KITE already disable Param so ReScan do nothing even Postgres get this function */
	return;
}

/*
 * kiteEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
kiteEndForeignScan(ForeignScanState *node)
{
	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;

	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;

	/* Release remote connection */
	ReleaseConnection(fsstate->req);
	fsstate->req = NULL;

	if (fsstate->agg) {
		xrg_agg_destroy(fsstate->agg);
		fsstate->agg = 0;
	}

	if (fsstate->partial_agg) {
		xrg_agg_p_destroy(fsstate->partial_agg);
		fsstate->partial_agg = 0;
	}
	/* MemoryContexts will be deleted automatically. */
}

/*
 * kiteExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void
kiteExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	List	   *fdw_private;
	char	   *sql;
	char	   *relations;

	fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;

	/*
	 * Add names of relation handled by the foreign scan when the scan is a
	 * join
	 */
	if (list_length(fdw_private) > FdwScanPrivateRelations)
	{
		relations = strVal(list_nth(fdw_private, FdwScanPrivateRelations));
		ExplainPropertyText("Relations", relations, es);
	}

	/*
	 * Add remote query, when VERBOSE option is specified.
	 */
	if (es->verbose)
	{
		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
		ExplainPropertyText("Remote SQL", sql, es);
	}
}


/*
 * estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign relation
 *		either a base relation or a join between foreign relations or an upper
 *		relation containing foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 * fpextra specifies additional post-scan/join-processing steps such as the
 * final sort and the LIMIT restriction.
 *
 * The function returns the cost and size estimates in p_rows, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void
estimate_path_cost_size(PlannerInfo *root,
						RelOptInfo *foreignrel,
						List *param_join_conds,
						List *pathkeys,
						PgFdwPathExtraData *fpextra,
						double *p_rows, int *p_width,
						Cost *p_startup_cost, Cost *p_total_cost)
{
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;
	double		rows;
	double		retrieved_rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/* Make sure the core code has set up the relation's reltarget */
	Assert(foreignrel->reltarget);

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction+join clauses.  Otherwise,
	 * estimate rows using whatever statistics we have locally, in a way
	 * similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		List	   *remote_param_join_conds;
		List	   *local_param_join_conds;
		StringInfoData sql;
		Selectivity local_sel;
		QualCost	local_cost;
		List	   *fdw_scan_tlist = NIL;
		List	   *remote_conds;

		/* KITE: required only to be passed to deparseSelectStmtForRel */
		/* Required only to be passed to deparseSelectStmtForRel */
		List	   *retrieved_attrs;
		List *retrieved_aggfnoids;
		List *retrieved_groupby_attrs;

		/*
		 * param_join_conds might contain both clauses that are safe to send
		 * across, and clauses that aren't.
		 */
		classifyConditions(root, foreignrel, param_join_conds,
						   &remote_param_join_conds, &local_param_join_conds);

		/* Build the list of columns to be fetched from the foreign server. */
		if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
			fdw_scan_tlist = build_tlist_to_deparse(foreignrel);
		else
			fdw_scan_tlist = NIL;

		/*
		 * The complete list of remote conditions includes everything from
		 * baserestrictinfo plus any extra join_conds relevant to this
		 * particular path.
		 */
		remote_conds = list_concat(list_copy(remote_param_join_conds),
								   fpinfo->remote_conds);

		/*
		 * Construct EXPLAIN query including the desired SELECT, FROM, and
		 * WHERE clauses. Params and other-relation Vars are replaced by dummy
		 * values, so don't request params_list.
		 */
		initStringInfo(&sql);
		appendStringInfoString(&sql, "EXPLAIN ");
		deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
								remote_conds, pathkeys,
								fpextra ? fpextra->has_final_sort : false,
								fpextra ? fpextra->has_limit : false,
								false, &retrieved_attrs, NULL,
								&retrieved_aggfnoids, &retrieved_groupby_attrs);

		/* Get the remote estimate */
		/* KITE */
		rows = 0;
		width = 0;
		startup_cost = 0;
		total_cost = 0;

		retrieved_rows = rows;

		/* Factor in the selectivity of the locally-checked quals */
		local_sel = clauselist_selectivity(root,
										   local_param_join_conds,
										   foreignrel->relid,
										   JOIN_INNER,
										   NULL);
		local_sel *= fpinfo->local_conds_sel;

		rows = clamp_row_est(rows * local_sel);

		/* Add in the eval cost of the locally-checked quals */
		startup_cost += fpinfo->local_conds_cost.startup;
		total_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
		cost_qual_eval(&local_cost, local_param_join_conds, root);
		startup_cost += local_cost.startup;
		total_cost += local_cost.per_tuple * retrieved_rows;

		/*
		 * Add in tlist eval cost for each output row.  In case of an
		 * aggregate, some of the tlist expressions such as grouping
		 * expressions will be evaluated remotely, so adjust the costs.
		 */
		startup_cost += foreignrel->reltarget->cost.startup;
		total_cost += foreignrel->reltarget->cost.startup;
		total_cost += foreignrel->reltarget->cost.per_tuple * rows;
		if (IS_UPPER_REL(foreignrel))
		{
			QualCost	tlist_cost;

			cost_qual_eval(&tlist_cost, fdw_scan_tlist, root);
			startup_cost -= tlist_cost.startup;
			total_cost -= tlist_cost.startup;
			total_cost -= tlist_cost.per_tuple * rows;
		}
	}
	else
	{
		Cost		run_cost = 0;

		/*
		 * We don't support join conditions in this mode (hence, no
		 * parameterized paths can be made).
		 */
		Assert(param_join_conds == NIL);

		/*
		 * We will come here again and again with different set of pathkeys or
		 * additional post-scan/join-processing steps that caller wants to
		 * cost.  We don't need to calculate the cost/size estimates for the
		 * underlying scan, join, or grouping each time.  Instead, use those
		 * estimates if we have cached them already.
		 */
		if (fpinfo->rel_startup_cost >= 0 && fpinfo->rel_total_cost >= 0)
		{
			Assert(fpinfo->retrieved_rows >= 1);

			rows = fpinfo->rows;
			retrieved_rows = fpinfo->retrieved_rows;
			width = fpinfo->width;
			startup_cost = fpinfo->rel_startup_cost;
			run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;

			/*
			 * If we estimate the costs of a foreign scan or a foreign join
			 * with additional post-scan/join-processing steps, the scan or
			 * join costs obtained from the cache wouldn't yet contain the
			 * eval costs for the final scan/join target, which would've been
			 * updated by apply_scanjoin_target_to_paths(); add the eval costs
			 * now.
			 */
			if (fpextra && !IS_UPPER_REL(foreignrel))
			{
				/* Shouldn't get here unless we have LIMIT */
				Assert(fpextra->has_limit);
				Assert(foreignrel->reloptkind == RELOPT_BASEREL ||
					   foreignrel->reloptkind == RELOPT_JOINREL);
				startup_cost += foreignrel->reltarget->cost.startup;
				run_cost += foreignrel->reltarget->cost.per_tuple * rows;
			}
		}
		else if (IS_JOIN_REL(foreignrel))
		{
			PgFdwRelationInfo *fpinfo_i;
			PgFdwRelationInfo *fpinfo_o;
			QualCost	join_cost;
			QualCost	remote_conds_cost;
			double		nrows;

			/* Use rows/width estimates made by the core code. */
			rows = foreignrel->rows;
			width = foreignrel->reltarget->width;

			/* For join we expect inner and outer relations set */
			Assert(fpinfo->innerrel && fpinfo->outerrel);

			fpinfo_i = (PgFdwRelationInfo *) fpinfo->innerrel->fdw_private;
			fpinfo_o = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;

			/* Estimate of number of rows in cross product */
			nrows = fpinfo_i->rows * fpinfo_o->rows;

			/*
			 * Back into an estimate of the number of retrieved rows.  Just in
			 * case this is nuts, clamp to at most nrow.
			 */
			retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
			retrieved_rows = Min(retrieved_rows, nrows);

			/*
			 * The cost of foreign join is estimated as cost of generating
			 * rows for the joining relations + cost for applying quals on the
			 * rows.
			 */

			/*
			 * Calculate the cost of clauses pushed down to the foreign server
			 */
			cost_qual_eval(&remote_conds_cost, fpinfo->remote_conds, root);
			/* Calculate the cost of applying join clauses */
			cost_qual_eval(&join_cost, fpinfo->joinclauses, root);

			/*
			 * Startup cost includes startup cost of joining relations and the
			 * startup cost for join and other clauses. We do not include the
			 * startup cost specific to join strategy (e.g. setting up hash
			 * tables) since we do not know what strategy the foreign server
			 * is going to use.
			 */
			startup_cost = fpinfo_i->rel_startup_cost + fpinfo_o->rel_startup_cost;
			startup_cost += join_cost.startup;
			startup_cost += remote_conds_cost.startup;
			startup_cost += fpinfo->local_conds_cost.startup;

			/*
			 * Run time cost includes:
			 *
			 * 1. Run time cost (total_cost - startup_cost) of relations being
			 * joined
			 *
			 * 2. Run time cost of applying join clauses on the cross product
			 * of the joining relations.
			 *
			 * 3. Run time cost of applying pushed down other clauses on the
			 * result of join
			 *
			 * 4. Run time cost of applying nonpushable other clauses locally
			 * on the result fetched from the foreign server.
			 */
			run_cost = fpinfo_i->rel_total_cost - fpinfo_i->rel_startup_cost;
			run_cost += fpinfo_o->rel_total_cost - fpinfo_o->rel_startup_cost;
			run_cost += nrows * join_cost.per_tuple;
			nrows = clamp_row_est(nrows * fpinfo->joinclause_sel);
			run_cost += nrows * remote_conds_cost.per_tuple;
			run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;

			/* Add in tlist eval cost for each output row */
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		}
		else if (IS_UPPER_REL(foreignrel))
		{
			RelOptInfo *outerrel = fpinfo->outerrel;
			PgFdwRelationInfo *ofpinfo;
			AggClauseCosts aggcosts;
			double		input_rows;
			int			numGroupCols;
			double		numGroups = 1;

			/* The upper relation should have its outer relation set */
			Assert(outerrel);
			/* and that outer relation should have its reltarget set */
			Assert(outerrel->reltarget);

			/*
			 * This cost model is mixture of costing done for sorted and
			 * hashed aggregates in cost_agg().  We are not sure which
			 * strategy will be considered at remote side, thus for
			 * simplicity, we put all startup related costs in startup_cost
			 * and all finalization and run cost are added in total_cost.
			 */

			ofpinfo = (PgFdwRelationInfo *) outerrel->fdw_private;

			/* Get rows from input rel */
			input_rows = ofpinfo->rows;

			/* Collect statistics about aggregates for estimating costs. */
			MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
			if (root->parse->hasAggs)
			{
				get_agg_clause_costs(root, (Node *) fpinfo->grouped_tlist,
									 AGGSPLIT_SIMPLE, &aggcosts);

				/*
				 * The cost of aggregates in the HAVING qual will be the same
				 * for each child as it is for the parent, so there's no need
				 * to use a translated version of havingQual.
				 */
				get_agg_clause_costs(root, (Node *) root->parse->havingQual,
									 AGGSPLIT_SIMPLE, &aggcosts);
			}

			/* Get number of grouping columns and possible number of groups */
			numGroupCols = list_length(root->parse->groupClause);
			numGroups = estimate_num_groups(root,
											get_sortgrouplist_exprs(root->parse->groupClause,
																	fpinfo->grouped_tlist),
											input_rows, NULL);

			/*
			 * Get the retrieved_rows and rows estimates.  If there are HAVING
			 * quals, account for their selectivity.
			 */
			if (root->parse->havingQual)
			{
				/* Factor in the selectivity of the remotely-checked quals */
				retrieved_rows =
					clamp_row_est(numGroups *
								  clauselist_selectivity(root,
														 fpinfo->remote_conds,
														 0,
														 JOIN_INNER,
														 NULL));
				/* Factor in the selectivity of the locally-checked quals */
				rows = clamp_row_est(retrieved_rows * fpinfo->local_conds_sel);
			}
			else
			{
				rows = retrieved_rows = numGroups;
			}

			/* Use width estimate made by the core code. */
			width = foreignrel->reltarget->width;

			/*-----
			 * Startup cost includes:
			 *	  1. Startup cost for underneath input relation, adjusted for
			 *	     tlist replacement by apply_scanjoin_target_to_paths()
			 *	  2. Cost of performing aggregation, per cost_agg()
			 *-----
			 */
			startup_cost = ofpinfo->rel_startup_cost;
			startup_cost += outerrel->reltarget->cost.startup;
			startup_cost += aggcosts.transCost.startup;
			startup_cost += aggcosts.transCost.per_tuple * input_rows;
			startup_cost += aggcosts.finalCost.startup;
			startup_cost += (cpu_operator_cost * numGroupCols) * input_rows;

			/*-----
			 * Run time cost includes:
			 *	  1. Run time cost of underneath input relation, adjusted for
			 *	     tlist replacement by apply_scanjoin_target_to_paths()
			 *	  2. Run time cost of performing aggregation, per cost_agg()
			 *-----
			 */
			run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
			run_cost += outerrel->reltarget->cost.per_tuple * input_rows;
			run_cost += aggcosts.finalCost.per_tuple * numGroups;
			run_cost += cpu_tuple_cost * numGroups;

			/* Account for the eval cost of HAVING quals, if any */
			if (root->parse->havingQual)
			{
				QualCost	remote_cost;

				/* Add in the eval cost of the remotely-checked quals */
				cost_qual_eval(&remote_cost, fpinfo->remote_conds, root);
				startup_cost += remote_cost.startup;
				run_cost += remote_cost.per_tuple * numGroups;
				/* Add in the eval cost of the locally-checked quals */
				startup_cost += fpinfo->local_conds_cost.startup;
				run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
			}

			/* Add in tlist eval cost for each output row */
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		}
		else
		{
			Cost		cpu_per_tuple;

			/* Use rows/width estimates made by set_baserel_size_estimates. */
			rows = foreignrel->rows;
			width = foreignrel->reltarget->width;

			/*
			 * Back into an estimate of the number of retrieved rows.  Just in
			 * case this is nuts, clamp to at most foreignrel->tuples.
			 */
			retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
			retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

			/*
			 * Cost as though this were a seqscan, which is pessimistic.  We
			 * effectively imagine the local_conds are being evaluated
			 * remotely, too.
			 */
			startup_cost = 0;
			run_cost = 0;
			run_cost += seq_page_cost * foreignrel->pages;

			startup_cost += foreignrel->baserestrictcost.startup;
			cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
			run_cost += cpu_per_tuple * foreignrel->tuples;

			/* Add in tlist eval cost for each output row */
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		}

		/*
		 * Without remote estimates, we have no real way to estimate the cost
		 * of generating sorted output.  It could be free if the query plan
		 * the remote side would have chosen generates properly-sorted output
		 * anyway, but in most cases it will cost something.  Estimate a value
		 * high enough that we won't pick the sorted path when the ordering
		 * isn't locally useful, but low enough that we'll err on the side of
		 * pushing down the ORDER BY clause when it's useful to do so.
		 */
		if (pathkeys != NIL)
		{
			if (IS_UPPER_REL(foreignrel))
			{
				Assert(foreignrel->reloptkind == RELOPT_UPPER_REL &&
					   fpinfo->stage == UPPERREL_GROUP_AGG);
				adjust_foreign_grouping_path_cost(root, pathkeys,
												  retrieved_rows, width,
												  fpextra->limit_tuples,
												  &startup_cost, &run_cost);
			}
			else
			{
				startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
				run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			}
		}

		total_cost = startup_cost + run_cost;

		/* Adjust the cost estimates if we have LIMIT */
		if (fpextra && fpextra->has_limit)
		{
			adjust_limit_rows_costs(&rows, &startup_cost, &total_cost,
									fpextra->offset_est, fpextra->count_est);
			retrieved_rows = rows;
		}
	}

	/*
	 * If this includes the final sort step, the given target, which will be
	 * applied to the resulting path, might have different expressions from
	 * the foreignrel's reltarget (see make_sort_input_target()); adjust tlist
	 * eval costs.
	 */
	if (fpextra && fpextra->has_final_sort &&
		fpextra->target != foreignrel->reltarget)
	{
		QualCost	oldcost = foreignrel->reltarget->cost;
		QualCost	newcost = fpextra->target->cost;

		startup_cost += newcost.startup - oldcost.startup;
		total_cost += newcost.startup - oldcost.startup;
		total_cost += (newcost.per_tuple - oldcost.per_tuple) * rows;
	}

	/*
	 * Cache the retrieved rows and cost estimates for scans, joins, or
	 * groupings without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps, before adding the costs for
	 * transferring data from the foreign server.  These estimates are useful
	 * for costing remote joins involving this relation or costing other
	 * remote operations on this relation such as remote sorts and remote
	 * LIMIT restrictions, when the costs can not be obtained from the foreign
	 * server.  This function will be called at least once for every foreign
	 * relation without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps.
	 */
	if (pathkeys == NIL && param_join_conds == NIL && fpextra == NULL)
	{
		fpinfo->retrieved_rows = retrieved_rows;
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/*
	 * If we have LIMIT, we should prefer performing the restriction remotely
	 * rather than locally, as the former avoids extra row fetches from the
	 * remote that the latter might cause.  But since the core code doesn't
	 * account for such fetches when estimating the costs of the local
	 * restriction (see create_limit_path()), there would be no difference
	 * between the costs of the local restriction and the costs of the remote
	 * restriction estimated above if we don't use remote estimates (except
	 * for the case where the foreignrel is a grouping relation, the given
	 * pathkeys is not NIL, and the effects of a bounded sort for that rel is
	 * accounted for in costing the remote restriction).  Tweak the costs of
	 * the remote restriction to ensure we'll prefer it if LIMIT is a useful
	 * one.
	 */
	if (!fpinfo->use_remote_estimate &&
		fpextra && fpextra->has_limit &&
		fpextra->limit_tuples > 0 &&
		fpextra->limit_tuples < fpinfo->rows)
	{
		Assert(fpinfo->rows > 0);
		total_cost -= (total_cost - startup_cost) * 0.05 *
			(fpinfo->rows - fpextra->limit_tuples) / fpinfo->rows;
	}

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}

/*
 * Adjust the cost estimates of a foreign grouping path to include the cost of
 * generating properly-sorted output.
 */
static void
adjust_foreign_grouping_path_cost(PlannerInfo *root,
								  List *pathkeys,
								  double retrieved_rows,
								  double width,
								  double limit_tuples,
								  Cost *p_startup_cost,
								  Cost *p_run_cost)
{
	/*
	 * If the GROUP BY clause isn't sort-able, the plan chosen by the remote
	 * side is unlikely to generate properly-sorted output, so it would need
	 * an explicit sort; adjust the given costs with cost_sort().  Likewise,
	 * if the GROUP BY clause is sort-able but isn't a superset of the given
	 * pathkeys, adjust the costs with that function.  Otherwise, adjust the
	 * costs by applying the same heuristic as for the scan or join case.
	 */
	if (!grouping_is_sortable(root->parse->groupClause) ||
		!pathkeys_contained_in(pathkeys, root->group_pathkeys))
	{
		Path		sort_path;	/* dummy for result of cost_sort */

		cost_sort(&sort_path,
				  root,
				  pathkeys,
				  *p_startup_cost + *p_run_cost,
				  retrieved_rows,
				  width,
				  0.0,
				  work_mem,
				  limit_tuples);

		*p_startup_cost = sort_path.startup_cost;
		*p_run_cost = sort_path.total_cost - sort_path.startup_cost;
	}
	else
	{
		/*
		 * The default extra cost seems too large for foreign-grouping cases;
		 * add 1/4th of that default.
		 */
		double		sort_multiplier = 1.0 + (DEFAULT_FDW_SORT_MULTIPLIER
											 - 1.0) * 0.25;

		*p_startup_cost *= sort_multiplier;
		*p_run_cost *= sort_multiplier;
	}
}

/*
 * Detect whether we want to process an EquivalenceClass member.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
static bool
ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
						  EquivalenceClass *ec, EquivalenceMember *em,
						  void *arg)
{
	ec_member_foreign_arg *state = (ec_member_foreign_arg *) arg;
	Expr	   *expr = em->em_expr;

	/*
	 * If we've identified what we're processing in the current scan, we only
	 * want to match that expression.
	 */
	if (state->current != NULL)
		return equal(expr, state->current);

	/*
	 * Otherwise, ignore anything we've already processed.
	 */
	if (list_member(state->already_used, expr))
		return false;

	/* This is the new target to process. */
	state->current = expr;
	return true;
}

/*
 * Create cursor for node's query with current parameter values.
 */
static void
create_cursor(ForeignScanState *node)
{
	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int			numParams = fsstate->numParams;
	const char **values = fsstate->param_values;
	kite_request_t *req = fsstate->req;
	char errmsg[1024];

	/*
	 * Construct array of query parameter values in text format.  We do the
	 * conversions in the short-lived per-tuple context, so as not to cause a
	 * memory leak over repeated scans.
	 */
	if (numParams > 0)
	{
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		process_query_params(econtext,
							 fsstate->param_flinfo,
							 fsstate->param_exprs,
							 values);

		MemoryContextSwitchTo(oldcontext);
	}

	elog(LOG, "CREATE CURSOR fragid = %d, fragcnt = %d", fsstate->fragid, fsstate->fragcnt);
	if (fsstate->fragid >= 0 && fsstate->fragid >= fsstate->fragcnt) {
	        /* Mark the cursor as created, and show no tuples have been retrieved */
       		fsstate->cursor_exists = true;
        	fsstate->tuples = NULL;
        	fsstate->num_tuples = 0;
        	fsstate->next_tuple = 0;
        	fsstate->fetch_ct_2 = 0;
        	fsstate->eof_reached = true;
		req->hdl = NULL;
		return;
	}


	/* kite_submit */
	req->hdl = kite_submit(req->host, fsstate->schema, fsstate->query, fsstate->fragid, fsstate->fragcnt, fsstate->fspec, errmsg, sizeof(errmsg));
	if (!req->hdl) {
		elog(ERROR, "kite_submit failed");
		return;
	}

	/* Mark the cursor as created, and show no tuples have been retrieved */
	fsstate->cursor_exists = true;
	fsstate->tuples = NULL;
	fsstate->num_tuples = 0;
	fsstate->next_tuple = 0;
	fsstate->fetch_ct_2 = 0;
	fsstate->eof_reached = false;

}

/*
 * Fetch some more rows from the node's cursor.
 */
static void
fetch_more_data(ForeignScanState *node)
{
	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;
	char errmsg[1024];
	MemoryContext oldcontext;

	/*
	 * We'll store the tuples in the batch_cxt.  First, flush the previous
	 * batch.
	 */
	fsstate->tuples = NULL;
	MemoryContextReset(fsstate->batch_cxt);
	oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt);

	PG_TRY();
	{
		kite_handle_t *hdl = fsstate->req->hdl;
		int numrows;
		int i;

		if (fsstate->agg) {
			int batchsz = fsstate->fetch_size;

			xrg_agg_fetch(fsstate->agg, hdl);

			numrows = 0;
			fsstate->tuples = (HeapTuple *)palloc0(batchsz * sizeof(HeapTuple));
			fsstate->num_tuples = batchsz;
			fsstate->next_tuple = 0;
			for (i = 0; i < batchsz; i++) {
				Assert(IsA(node->ss.ps.plan, ForeignScan));
				fsstate->tuples[i] = make_tuple_from_agg(fsstate->agg,
					i,
					fsstate->rel,
					fsstate->attinmeta,
					fsstate->retrieved_attrs,
					node,
					fsstate->temp_cxt);
				if (!fsstate->tuples[i]) {
					break;
				}
				numrows++;
			}

			fsstate->num_tuples = numrows;
			fsstate->next_tuple = 0;
			fsstate->eof_reached = (numrows < batchsz);

		} else {
			int batchsz = fsstate->fetch_size;
			xrg_iter_t *iter = 0;
			int e = 0;
			numrows = 0;

			fsstate->tuples = (HeapTuple *)palloc0(batchsz * sizeof(HeapTuple));
			fsstate->num_tuples = batchsz;
			fsstate->next_tuple = 0;

			for (i = 0; i < batchsz && fsstate->eof_reached == false; i++) {
				Assert(IsA(node->ss.ps.plan, ForeignScan));

				e = kite_next_row(hdl, &iter, errmsg, sizeof(errmsg));
				if (e == 0) {
					if (iter == 0) {
						fsstate->eof_reached = true;
						break;
					}

					if (fsstate->partial_agg) {
						fsstate->tuples[i] = make_tuple_from_partial_agg(fsstate->partial_agg, 
							iter,
							i,
							fsstate->rel,
							fsstate->attinmeta,
							fsstate->retrieved_attrs,
							node,
							fsstate->temp_cxt);
						numrows++;


					} else {
						fsstate->tuples[i] = make_tuple_from_result_row(iter,
							i,
							fsstate->rel,
							fsstate->attinmeta,
							fsstate->retrieved_attrs,
							node,
							fsstate->temp_cxt);
						numrows++;

					}
				} else {
					// error
					elog(ERROR, "kite_next_row failed");
					fsstate->eof_reached = true;
				}
			}

			//elog(LOG, "fetch_more_data %d row", numrows);
			fsstate->num_tuples = numrows;
			fsstate->next_tuple = 0;

			/* Update fetch_ct_2 */
			if (fsstate->fetch_ct_2 < 2)
				fsstate->fetch_ct_2++;
		}
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
int
set_transmission_modes(void)
{
	int			nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void) set_config_option("datestyle", "ISO",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void) set_config_option("intervalstyle", "postgres",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void) set_config_option("extra_float_digits", "3",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);

	/*
	 * In addition force restrictive search_path, in case there are any
	 * regproc or similar constants to be printed.
	 */
	(void) set_config_option("search_path", "pg_catalog",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void
reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}

/*
 * Construct array of query parameter values in text format.
 */
static void
process_query_params(ExprContext *econtext,
					 FmgrInfo *param_flinfo,
					 List *param_exprs,
					 const char **param_values)
{
	int			nestlevel;
	int			i;
	ListCell   *lc;

	nestlevel = set_transmission_modes();

	i = 0;
	foreach(lc, param_exprs)
	{
		ExprState  *expr_state = (ExprState *) lfirst(lc);
		Datum		expr_value;
		bool		isNull;

		/* Evaluate the parameter expression */
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);

		/*
		 * Get string representation of each parameter value by invoking
		 * type-specific output function, unless the value is null.
		 */
		if (isNull)
			param_values[i] = NULL;
		else
			param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);

		i++;
	}

	reset_transmission_modes(nestlevel);
}

/*
 * kiteAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
kiteAnalyzeForeignTable(Relation relation,
							AcquireSampleRowsFunc *func,
							BlockNumber *totalpages)
{
	double totalrows = 0;

	/* Return the row-analysis function pointer */
	*func = kiteAcquireSampleRowsFunc;

	//return kite_get_relation_stats(relation, totalpages, &totalrows);
	// give fake number and get the stats in the later stage
	*totalpages = 100;

	return true;
}

/*
 * Acquire a random sample of rows from foreign table managed by kite_fdw.
 *
 * We fetch the whole table from the remote side and pick out some sample rows.
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the table and return it into
 * *totalrows.  Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the table.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
static int
kiteAcquireSampleRowsFunc(Relation relation, int elevel,
							  HeapTuple *rows, int targrows,
							  double *totalrows,
							  double *totaldeadrows)
{
	PgFdwAnalyzeState astate;
	PgFdwRelationInfo *fpinfo;
	char errormsg[1024];
	ForeignTable *table;
	UserMapping *user;
	kite_request_t *req;
	StringInfoData sql, schema;
	List *retrieved_attrs;
	BlockNumber totalpages = 0;
	xrg_iter_t *iter = 0;
	int e = 0;
	MemoryContext oldcontext;
	kite_filespec_t *fspec;

	astate.rel = relation;
	astate.attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(relation));
	astate.rows = rows;
	astate.targrows = targrows;
	astate.numrows = 0;
	astate.samplerows = 0;

	fpinfo = (PgFdwRelationInfo *)palloc0(sizeof(PgFdwRelationInfo));
	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(relation->rd_id);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	fpinfo->fragcnt = -1;
	fpinfo->csv_delim = ',';
	fpinfo->csv_quote = '"';
	fpinfo->csv_escape = '"';
	fpinfo->csv_nullstr = 0;
	fpinfo->csv_header = false;
	apply_server_options(fpinfo);
	apply_table_options(fpinfo);

        /*
         * build filespec for KITE
         */
        fspec = (kite_filespec_t*) palloc0(sizeof(kite_filespec_t));
        if (! fpinfo->fmt) {
                elog(ERROR, "table option fmt is not specified");
        }

        strcpy(fspec->fmt, fpinfo->fmt);
        fspec->u.csv.delim = fpinfo->csv_delim;
        fspec->u.csv.quote = fpinfo->csv_quote;
        fspec->u.csv.escape = fpinfo->csv_escape;
        if (fpinfo->csv_nullstr) {
                strcpy(fspec->u.csv.nullstr, fpinfo->csv_nullstr);
        } else {
                *fspec->u.csv.nullstr = 0;
        }
        fspec->u.csv.header_line = fpinfo->csv_header;


	if (!kite_get_relation_stats(fpinfo, relation, fspec, &totalpages, &astate.samplerows)) {
		elog(ERROR, "kite_Get_relation_stats failed");
		return 0;
	}

	/* Remember ANALYZE context, and create a per-tuple temp context */
	astate.anl_cxt = CurrentMemoryContext;
	astate.temp_cxt = AllocSetContextCreate(CurrentMemoryContext,
		"kite_fdw temporary data",
		ALLOCSET_SMALL_SIZES);

	/*
	 * Get the connection to use.  We do the remote access as the table's
	 * owner, even if the ANALYZE was started by some other user.
	 */
	table = GetForeignTable(RelationGetRelid(relation));
	user = GetUserMapping(relation->rd_rel->relowner, table->serverid);
	req = GetConnection(user, false);

	/*
	 * Construct command to get page count for relation.
	 */
	retrieved_attrs = NULL;
	initStringInfo(&sql);
	deparseAnalyzeSql(&sql, relation, &retrieved_attrs, targrows, *totalrows);

	/*
         * build the schema for KITE
	 */
	{
		TupleDesc tupdesc = RelationGetDescr(relation);
		initStringInfo(&schema);
		kite_build_schema(&schema, tupdesc);
	}

	req->hdl = kite_submit(req->host, schema.data, sql.data, -1, fpinfo->fragcnt, fspec, errormsg, sizeof(errormsg));
	if (!req->hdl) {
		elog(ERROR, "kite_submit failed -- %s", errormsg);
		return 0;
	}

	PG_TRY();
	{

		oldcontext = MemoryContextSwitchTo(astate.anl_cxt);
		for (int i = 0; i < targrows; i++) {

			e = kite_next_row(req->hdl, &iter, errormsg, sizeof(errormsg));
			if (e == 0) {
				if (iter == 0) {
					break;
				}

				rows[i] = make_tuple_from_result_row(iter,
					i,
					astate.rel,
					astate.attinmeta,
					retrieved_attrs,
					NULL,
					astate.temp_cxt);

				astate.numrows++;
			} else {
				elog(ERROR, "kite_next_row failed");
			}
		}
		MemoryContextSwitchTo(oldcontext);

		elog(LOG, "Analyze sample received %d rows, targrows =%d", astate.numrows, astate.targrows);
	}
	PG_CATCH();
	{
		ReleaseConnection(req);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleaseConnection(req);

	/* We assume that we have no dead tuple */
	*totaldeadrows = 0;

	/* We've retrieved total number of rows from foreign server */
	*totalrows = astate.samplerows;

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
		(errmsg("\"%s\": table contains %.0f rows, %d rows in sample",
			RelationGetRelationName(relation),
			astate.samplerows, astate.numrows)));

	return astate.numrows;
}

static void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
								Path *epq_path)
{
	List	   *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell   *lc;

	useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach(lc, useful_pathkeys_list)
	{
		double		rows;
		int			width;
		Cost		startup_cost;
		Cost		total_cost;
		List	   *useful_pathkeys = lfirst(lc);
		Path	   *sorted_epq_path;

		estimate_path_cost_size(root, rel, NIL, useful_pathkeys, NULL,
								&rows, &width, &startup_cost, &total_cost);

		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys,
								   sorted_epq_path->pathkeys))
			sorted_epq_path = (Path *)
				create_sort_path(root,
								 rel,
								 sorted_epq_path,
								 useful_pathkeys,
								 -1.0);

		if (IS_SIMPLE_REL(rel))
			add_path(rel, (Path *)
					 create_foreignscan_path(root, rel,
											 NULL,
											 rows,
											 startup_cost,
											 total_cost,
											 useful_pathkeys,
											 rel->lateral_relids,
											 sorted_epq_path,
											 NIL));
		else
			add_path(rel, (Path *)
					 create_foreign_join_path(root, rel,
											  NULL,
											  rows,
											  startup_cost,
											  total_cost,
											  useful_pathkeys,
											  rel->lateral_relids,
											  sorted_epq_path,
											  NIL));
	}
}

/*
 * Parse options from foreign server and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_server_options(PgFdwRelationInfo *fpinfo)
{
	ListCell   *lc;

	foreach(lc, fpinfo->server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			fpinfo->use_remote_estimate = defGetBoolean(def);
		else if (strcmp(def->defname, "fdw_startup_cost") == 0)
			fpinfo->fdw_startup_cost = strtod(defGetString(def), NULL);
		else if (strcmp(def->defname, "fdw_tuple_cost") == 0)
			fpinfo->fdw_tuple_cost = strtod(defGetString(def), NULL);
		else if (strcmp(def->defname, "extensions") == 0)
			fpinfo->shippable_extensions =
				ExtractExtensionList(defGetString(def), false);
		else if (strcmp(def->defname, "fetch_size") == 0)
			fpinfo->fetch_size = strtol(defGetString(def), NULL, 10);
	}
}

/*
 * Parse options from foreign table and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_table_options(PgFdwRelationInfo *fpinfo)
{
	ListCell   *lc;

	foreach(lc, fpinfo->table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			fpinfo->use_remote_estimate = defGetBoolean(def);
		else if (strcmp(def->defname, "fetch_size") == 0)
			fpinfo->fetch_size = strtol(defGetString(def), NULL, 10);
		else if (strcmp(def->defname, "fmt") == 0)
			fpinfo->fmt = defGetString(def);
		else if (strcmp(def->defname, "csv_delimiter") == 0)
			fpinfo->csv_delim = *defGetString(def);
		else if (strcmp(def->defname, "csv_quote") == 0)
			fpinfo->csv_quote = *defGetString(def);
		else if (strcmp(def->defname, "csv_escape") == 0)
			fpinfo->csv_escape = *defGetString(def);
		else if (strcmp(def->defname, "csv_nullstr") == 0)
			fpinfo->csv_nullstr = defGetString(def);
		else if (strcmp(def->defname, "csv_header") == 0)
			fpinfo->csv_header = defGetBoolean(def);
		else if (strcmp(def->defname, "fragcnt") == 0)
			(void)parse_int(defGetString(def), &fpinfo->fragcnt, 0, NULL);
	}
}

/*
 * Merge FDW options from input relations into a new set of options for a join
 * or an upper rel.
 *
 * For a join relation, FDW-specific information about the inner and outer
 * relations is provided using fpinfo_i and fpinfo_o.  For an upper relation,
 * fpinfo_o provides the information for the input relation; fpinfo_i is
 * expected to NULL.
 */
static void
merge_fdw_options(PgFdwRelationInfo *fpinfo,
				  const PgFdwRelationInfo *fpinfo_o,
				  const PgFdwRelationInfo *fpinfo_i)
{
	/* We must always have fpinfo_o. */
	Assert(fpinfo_o);

	/* fpinfo_i may be NULL, but if present the servers must both match. */
	Assert(!fpinfo_i ||
		   fpinfo_i->server->serverid == fpinfo_o->server->serverid);

	/*
	 * Copy the server specific FDW options.  (For a join, both relations come
	 * from the same server, so the server options should have the same value
	 * for both relations.)
	 */
	fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;
	fpinfo->shippable_extensions = fpinfo_o->shippable_extensions;
	fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate;
	fpinfo->fetch_size = fpinfo_o->fetch_size;
	fpinfo->fragcnt = fpinfo_o->fragcnt;
	fpinfo->fmt = fpinfo_o->fmt;
	fpinfo->csv_delim = fpinfo_o->csv_delim;
	fpinfo->csv_quote = fpinfo_o->csv_quote;
	fpinfo->csv_escape = fpinfo_o->csv_escape;
	fpinfo->csv_nullstr = fpinfo_o->csv_nullstr;
	fpinfo->csv_header = fpinfo_o->csv_header;

	/* Merge the table level options from either side of the join. */
	if (fpinfo_i)
	{
		/*
		 * We'll prefer to use remote estimates for this join if any table
		 * from either side of the join is using remote estimates.  This is
		 * most likely going to be preferred since they're already willing to
		 * pay the price of a round trip to get the remote EXPLAIN.  In any
		 * case it's not entirely clear how we might otherwise handle this
		 * best.
		 */
		fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate ||
			fpinfo_i->use_remote_estimate;

		/*
		 * Set fetch size to maximum of the joining sides, since we are
		 * expecting the rows returned by the join to be proportional to the
		 * relation sizes.
		 */
		fpinfo->fetch_size = Max(fpinfo_o->fetch_size, fpinfo_i->fetch_size);

		/* KITE */
		fpinfo->fragcnt = Max(fpinfo_o->fragcnt, fpinfo_i->fragcnt);
		fpinfo->fmt = fpinfo_o->fmt ? fpinfo_o->fmt : fpinfo_i->fmt;
		fpinfo->csv_delim = fpinfo_o->csv_delim ? fpinfo_o->csv_delim : fpinfo_i->csv_delim;
		fpinfo->csv_escape = fpinfo_o->csv_escape ? fpinfo_o->csv_escape : fpinfo_i->csv_escape;
		fpinfo->csv_quote = fpinfo_o->csv_quote ? fpinfo_o->csv_quote: fpinfo_i->csv_quote;
		fpinfo->csv_nullstr = fpinfo_o->csv_nullstr ? fpinfo_o->csv_nullstr : fpinfo_i->csv_nullstr;
		fpinfo->csv_header = fpinfo_o->csv_header || fpinfo_i->csv_header;
	}
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to PgFdwRelationInfo of the input relation.
 */
static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
					Node *havingQual)
{
	Query	   *query = root->parse;
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) grouped_rel->fdw_private;
	PathTarget *grouping_target = grouped_rel->reltarget;
	PgFdwRelationInfo *ofpinfo;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;

	/* We currently don't support pushing Grouping Sets. */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;

	/*
	 * If underlying scan relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Examine grouping expressions, as well as other expressions we'd need to
	 * compute, and check whether they are safe to push down to the foreign
	 * server.  All GROUP BY expressions will be part of the grouping target
	 * and thus there is no need to search for them separately.  Add grouping
	 * expressions into target list which will be passed to foreign server.
	 *
	 * A tricky fine point is that we must not put any expression into the
	 * target list that is just a foreign param (that is, something that
	 * deparse.c would conclude has to be sent to the foreign server).  If we
	 * do, the expression will also appear in the fdw_exprs list of the plan
	 * node, and setrefs.c will get confused and decide that the fdw_exprs
	 * entry is actually a reference to the fdw_scan_tlist entry, resulting in
	 * a broken plan.  Somewhat oddly, it's OK if the expression contains such
	 * a node, as long as it's not at top level; then no match is possible.
	 */
	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell   *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any GROUP BY expression is not shippable, then we cannot
			 * push down aggregation to the foreign server.
			 */
			if (!is_foreign_expr(root, grouped_rel, expr))
				return false;

			/*
			 * If it would be a foreign param, we can't put it into the tlist,
			 * so we have to fail.
			 */
			if (is_foreign_param(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/*
			 * Non-grouping expression we need to compute.  Can we ship it
			 * as-is to the foreign server?
			 */
			if (is_foreign_expr(root, grouped_rel, expr) &&
				!is_foreign_param(root, grouped_rel, expr))
			{
				/* Yes, so add to tlist as-is; OK to suppress duplicates */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				/* Not pushable as a whole; extract its Vars and aggregates */
				List	   *aggvars;

				aggvars = pull_var_clause((Node *) expr,
										  PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the foreign server.  (We
				 * don't have to check is_foreign_param, since that certainly
				 * won't return true for any such expression.)
				 */
				if (!is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain Vars
				 * outside an aggregate can be ignored, because they should be
				 * either same as some GROUP BY column or part of some GROUP
				 * BY expression.  In either case, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * including plain Vars in the tlist when they do not match a
				 * GROUP BY column would cause the foreign server to complain
				 * that the shipped query is invalid.
				 */
				foreach(l, aggvars)
				{
					Expr	   *expr = (Expr *) lfirst(l);

					if (IsA(expr, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(expr));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable HAVING clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (havingQual)
	{
		ListCell   *lc;

		foreach(lc, (List *) havingQual)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
			rinfo = make_restrictinfo(root,
									  expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
#if 1

			/* KITE cannot handle HAVING */
			fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
#else
			if (is_foreign_expr(root, grouped_rel, expr))
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
#endif
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List	   *aggvars = NIL;
		ListCell   *lc;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.  Again, we need not check
			 * is_foreign_param for a foreign aggregate.
			 */
			if (IsA(expr, Aggref))
			{
				if (!is_foreign_expr(root, grouped_rel, expr))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "Aggregate on (%s)",
					 ofpinfo->relation_name->data);

	return true;
}

/*
 * kiteGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 */
static void
kiteGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
							 RelOptInfo *input_rel, RelOptInfo *output_rel,
							 void *extra)
{
	PgFdwRelationInfo *fpinfo;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((PgFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if ((stage != UPPERREL_GROUP_AGG &&
		stage != UPPERREL_FINAL &&
		stage != UPPERREL_CDB_FIRST_STAGE_GROUP_AGG) ||
		output_rel->fdw_private)
		return;

	fpinfo = (PgFdwRelationInfo *) palloc0(sizeof(PgFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	output_rel->fdw_private = fpinfo;

	switch (stage)
	{
		case UPPERREL_CDB_FIRST_STAGE_GROUP_AGG:
			/* It's unsafe to push having statements with partial aggregates */
			if(((GroupPathExtraData *) extra)->havingQual) {
				return;
			}
			/* Fall through */
			/* Partial agg push down path */
		case UPPERREL_GROUP_AGG:
			add_foreign_grouping_paths(root, input_rel, output_rel,
									   (GroupPathExtraData *) extra);
			break;
		case UPPERREL_FINAL:
			add_foreign_final_paths(root, input_rel, output_rel,
									(FinalPathExtraData *) extra);
			break;
		default:
			elog(ERROR, "unexpected upper relation: %d", (int) stage);
			break;
	}
}

/*
 * add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
						   RelOptInfo *grouped_rel,
						   GroupPathExtraData *extra)
{
	Query	   *parse = root->parse;
	PgFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	PgFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

	Assert(extra->patype == PARTITIONWISE_AGGREGATE_NONE ||
		   extra->patype == PARTITIONWISE_AGGREGATE_FULL);

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->user = ifpinfo->user;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * Assess if it is safe to push down aggregation and grouping.
	 *
	 * Use HAVING qual from extra. In case of child partition, it will have
	 * translated Vars.
	 */
	if (!foreign_grouping_ok(root, grouped_rel, extra->havingQual))
		return;

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  (Currently we create just a single
	 * path here, but in future it would be possible that we build more paths
	 * such as pre-sorted paths as in kiteGetForeignPaths and
	 * kiteGetForeignJoinPaths.)  The best we can do for these conditions
	 * is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 0,
													 JOIN_INNER,
													 NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/* Estimate the cost of push down */
	estimate_path_cost_size(root, grouped_rel, NIL, NIL, NULL,
							&rows, &width, &startup_cost, &total_cost);

	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/* Create and add foreign path to the grouping relation. */
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  rows,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL); /* no fdw_private */

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}

/*
 * add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
						RelOptInfo *final_rel,
						FinalPathExtraData *extra)
{
	Query	   *parse = root->parse;
	PgFdwRelationInfo *ifpinfo = (PgFdwRelationInfo *) input_rel->fdw_private;
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) final_rel->fdw_private;
	bool		has_final_sort = false;
	List	   *pathkeys = NIL;
	PgFdwPathExtraData *fpextra;
	bool		save_use_remote_estimate = false;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *final_path;

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * No work if there is no FOR UPDATE/SHARE clause and if there is no need
	 * to add a LIMIT node
	 */
	if (!parse->rowMarks && !extra->limit_needed)
		return;

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->user = ifpinfo->user;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * If there is no need to add a LIMIT node, there might be a ForeignPath
	 * in the input_rel's pathlist that implements all behavior of the query.
	 * Note: we would already have accounted for the query's FOR UPDATE/SHARE
	 * (if any) before we get here.
	 */
	if (!extra->limit_needed)
	{
		ListCell   *lc;

		Assert(parse->rowMarks);

		/*
		 * Grouping and aggregation are not supported with FOR UPDATE/SHARE,
		 * so the input_rel should be a base, join, or ordered relation; and
		 * if it's an ordered relation, its input relation should be a base or
		 * join relation.
		 */
		Assert(input_rel->reloptkind == RELOPT_BASEREL ||
			   input_rel->reloptkind == RELOPT_JOINREL ||
			   (input_rel->reloptkind == RELOPT_UPPER_REL &&
				ifpinfo->stage == UPPERREL_ORDERED &&
				(ifpinfo->outerrel->reloptkind == RELOPT_BASEREL ||
				 ifpinfo->outerrel->reloptkind == RELOPT_JOINREL)));

		foreach(lc, input_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);

			/*
			 * apply_scanjoin_target_to_paths() uses create_projection_path()
			 * to adjust each of its input paths if needed, whereas
			 * create_ordered_paths() uses apply_projection_to_path() to do
			 * that.  So the former might have put a ProjectionPath on top of
			 * the ForeignPath; look through ProjectionPath and see if the
			 * path underneath it is ForeignPath.
			 */
			if (IsA(path, ForeignPath) ||
				(IsA(path, ProjectionPath) &&
				 IsA(((ProjectionPath *) path)->subpath, ForeignPath)))
			{
				/*
				 * Create foreign final path; this gets rid of a
				 * no-longer-needed outer plan (if any), which makes the
				 * EXPLAIN output look cleaner
				 */
				final_path = create_foreign_upper_path(root,
													   path->parent,
													   path->pathtarget,
													   path->rows,
													   path->startup_cost,
													   path->total_cost,
													   path->pathkeys,
													   NULL,	/* no extra plan */
													   NULL);	/* no fdw_private */

				/* and add it to the final_rel */
				add_path(final_rel, (Path *) final_path);

				/* Safe to push down */
				fpinfo->pushdown_safe = true;

				return;
			}
		}

		/*
		 * If we get here it means no ForeignPaths; since we would already
		 * have considered pushing down all operations for the query to the
		 * remote server, give up on it.
		 */
		return;
	}

	Assert(extra->limit_needed);

	/*
	 * If the input_rel is an ordered relation, replace the input_rel with its
	 * input relation
	 */
	if (input_rel->reloptkind == RELOPT_UPPER_REL &&
		ifpinfo->stage == UPPERREL_ORDERED)
	{
		input_rel = ifpinfo->outerrel;
		ifpinfo = (PgFdwRelationInfo *) input_rel->fdw_private;
		has_final_sort = true;
		pathkeys = root->sort_pathkeys;
	}

	/* The input_rel should be a base, join, or grouping relation */
	Assert(input_rel->reloptkind == RELOPT_BASEREL ||
		   input_rel->reloptkind == RELOPT_JOINREL ||
		   (input_rel->reloptkind == RELOPT_UPPER_REL &&
			ifpinfo->stage == UPPERREL_GROUP_AGG));

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying base, join, or grouping relation to perform the final
	 * sort (if has_final_sort) and the LIMIT restriction remotely, which is
	 * stored into the fdw_private list of the resulting path.  (We
	 * re-estimate the costs of sorting the underlying relation, if
	 * has_final_sort.)
	 */

	/*
	 * Assess if it is safe to push down the LIMIT and OFFSET to the remote
	 * server
	 */

	/*
	 * If the underlying relation has any local conditions, the LIMIT/OFFSET
	 * cannot be pushed down.
	 */
	if (ifpinfo->local_conds)
		return;

	/*
	 * Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
	 * not safe to remote.
	 */
	if (!is_foreign_expr(root, input_rel, (Expr *) parse->limitOffset) ||
		!is_foreign_expr(root, input_rel, (Expr *) parse->limitCount))
		return;

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Construct PgFdwPathExtraData */
	fpextra = (PgFdwPathExtraData *) palloc0(sizeof(PgFdwPathExtraData));
	fpextra->target = root->upper_targets[UPPERREL_FINAL];
	fpextra->has_final_sort = has_final_sort;
	fpextra->has_limit = extra->limit_needed;
	fpextra->limit_tuples = extra->limit_tuples;
	fpextra->count_est = extra->count_est;
	fpextra->offset_est = extra->offset_est;

	/*
	 * Estimate the costs of performing the final sort and the LIMIT
	 * restriction remotely.  If has_final_sort is false, we wouldn't need to
	 * execute EXPLAIN anymore if use_remote_estimate, since the costs can be
	 * roughly estimated using the costs we already have for the underlying
	 * relation, in the same way as when use_remote_estimate is false.  Since
	 * it's pretty expensive to execute EXPLAIN, force use_remote_estimate to
	 * false in that case.
	 */
	if (!fpextra->has_final_sort)
	{
		save_use_remote_estimate = ifpinfo->use_remote_estimate;
		ifpinfo->use_remote_estimate = false;
	}
	estimate_path_cost_size(root, input_rel, NIL, pathkeys, fpextra,
							&rows, &width, &startup_cost, &total_cost);
	if (!fpextra->has_final_sort)
		ifpinfo->use_remote_estimate = save_use_remote_estimate;

	/*
	 * Build the fdw_private list that will be used by kiteGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(has_final_sort),
							 makeInteger(extra->limit_needed));

	/*
	 * Create foreign final path; this gets rid of a no-longer-needed outer
	 * plan (if any), which makes the EXPLAIN output look cleaner
	 */
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL,	/* no extra plan */
										   fdw_private);

	/* and add it to the final_rel */
	add_path(final_rel, (Path *) final_path);
}

/*
 * Create a tuple from the specified row of the PGresult.
 *
 * rel is the local representation of the foreign table, attinmeta is
 * conversion data for the rel's tupdesc, and retrieved_attrs is an
 * integer list of the table column numbers present in the PGresult.
 * fsstate is the ForeignScan plan node's execution state.
 * temp_context is a working context that can be reset after each tuple.
 *
 * Note: either rel or fsstate, but not both, can be NULL.  rel is NULL
 * if we're processing a remote join, while fsstate is NULL in a non-query
 * context such as ANALYZE, or if we're processing a non-scan query node.
 */
static HeapTuple
make_tuple_from_agg(xrg_agg_t *agg,
	int row,
	Relation rel,
	AttInMetadata *attinmeta,
	List *retrieved_attrs,
	ForeignScanState *fsstate,
	MemoryContext temp_context) {
	HeapTuple tuple;
	TupleDesc tupdesc;
	Datum *values;
	bool *nulls;
	ItemPointer ctid = NULL;
	ConversionLocation errpos;
	ErrorContextCallback errcallback;
	MemoryContext oldcontext;
	ListCell *lc;

	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	oldcontext = MemoryContextSwitchTo(temp_context);

	/*
	 * Get the tuple descriptor for the row.  Use the rel's tupdesc if rel is
	 * provided, otherwise look to the scan node's ScanTupleSlot.
	 */
	if (rel)
		tupdesc = RelationGetDescr(rel);
	else {
		Assert(fsstate);
		tupdesc = fsstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	values = (Datum *)palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *)palloc(tupdesc->natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	/*
	 * Set up and install callback to report where conversion error occurs.
	 */
#if 0
	errpos.cur_attno = 0;
	errpos.rel = rel;
	errpos.fsstate = fsstate;
	errcallback.callback = conversion_error_callback;
	errcallback.arg = (void *) &errpos;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;
#endif

	if (xrg_agg_get_next(agg, attinmeta, values, nulls, tupdesc->natts) != 0) {
		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(temp_context);
		return 0;
	}

#if 0
	/* Uninstall error context callback. */
	error_context_stack = errcallback.previous;

#endif

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/*
	 * If we have a CTID to return, install it in both t_self and t_ctid.
	 * t_self is the normal place, but if the tuple is converted to a
	 * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
	 * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
	 */
	if (ctid)
		tuple->t_self = tuple->t_data->t_ctid = *ctid;

	/*
	 * Stomp on the xmin, xmax, and cmin fields from the tuple created by
	 * heap_form_tuple.  heap_form_tuple actually creates the tuple with
	 * DatumTupleFields, not HeapTupleFields, but the executor expects
	 * HeapTupleFields and will happily extract system columns on that
	 * assumption.  If we don't do this then, for example, the tuple length
	 * ends up in the xmin field, which isn't what we want.
	 */
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);

	/* Clean up */
	MemoryContextReset(temp_context);

	return tuple;
}

static HeapTuple
make_tuple_from_partial_agg(xrg_agg_p_t *agg,
	xrg_iter_t *iter,
	int row,
	Relation rel,
	AttInMetadata *attinmeta,
	List *retrieved_attrs,
	ForeignScanState *fsstate,
	MemoryContext temp_context) {
	HeapTuple tuple;
	TupleDesc tupdesc;
	Datum *values;
	bool *nulls;
	ItemPointer ctid = NULL;
	ConversionLocation errpos;
	ErrorContextCallback errcallback;
	MemoryContext oldcontext;
	ListCell *lc;
	int j = 0;

	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	oldcontext = MemoryContextSwitchTo(temp_context);

	/*
	 * Get the tuple descriptor for the row.  Use the rel's tupdesc if rel is
	 * provided, otherwise look to the scan node's ScanTupleSlot.
	 */
	if (rel)
		tupdesc = RelationGetDescr(rel);
	else {
		Assert(fsstate);
		tupdesc = fsstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	values = (Datum *)palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *)palloc(tupdesc->natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	/*
	 * Set up and install callback to report where conversion error occurs.
	 */
	errpos.cur_attno = 0;
	errpos.rel = rel;
	errpos.fsstate = fsstate;
	errcallback.callback = conversion_error_callback;
	errcallback.arg = (void *)&errpos;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	if (xrg_agg_p_get_next(agg, iter, attinmeta, values, nulls, tupdesc->natts) != 0) {
                MemoryContextSwitchTo(oldcontext);
                MemoryContextReset(temp_context);
                return 0;
	}

	/* Uninstall error context callback. */
	error_context_stack = errcallback.previous;

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/*
	 * If we have a CTID to return, install it in both t_self and t_ctid.
	 * t_self is the normal place, but if the tuple is converted to a
	 * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
	 * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
	 */
	if (ctid)
		tuple->t_self = tuple->t_data->t_ctid = *ctid;

	/*
	 * Stomp on the xmin, xmax, and cmin fields from the tuple created by
	 * heap_form_tuple.  heap_form_tuple actually creates the tuple with
	 * DatumTupleFields, not HeapTupleFields, but the executor expects
	 * HeapTupleFields and will happily extract system columns on that
	 * assumption.  If we don't do this then, for example, the tuple length
	 * ends up in the xmin field, which isn't what we want.
	 */
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);

	/* Clean up */
	MemoryContextReset(temp_context);

	return tuple;
}

static HeapTuple
make_tuple_from_result_row(xrg_iter_t *iter,
	int row,
	Relation rel,
	AttInMetadata *attinmeta,
	List *retrieved_attrs,
	ForeignScanState *fsstate,
	MemoryContext temp_context) {
	HeapTuple tuple;
	TupleDesc tupdesc;
	Datum *values;
	bool *nulls;
	ItemPointer ctid = NULL;
	ConversionLocation errpos;
	ErrorContextCallback errcallback;
	MemoryContext oldcontext;
	ListCell *lc;
	int j = 0;

	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	oldcontext = MemoryContextSwitchTo(temp_context);

	/*
	 * Get the tuple descriptor for the row.  Use the rel's tupdesc if rel is
	 * provided, otherwise look to the scan node's ScanTupleSlot.
	 */
	if (rel)
		tupdesc = RelationGetDescr(rel);
	else {
		Assert(fsstate);
		tupdesc = fsstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	values = (Datum *)palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *)palloc(tupdesc->natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	/*
	 * Set up and install callback to report where conversion error occurs.
	 */
	errpos.cur_attno = 0;
	errpos.rel = rel;
	errpos.fsstate = fsstate;
	errcallback.callback = conversion_error_callback;
	errcallback.arg = (void *)&errpos;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	j = 0;
	foreach (lc, retrieved_attrs) {
		int i = lfirst_int(lc);
		var_decode(iter->value[j], *iter->flag[j], &iter->attr[j], attinmeta->atttypmods[i - 1],
			&values[i - 1], &nulls[i - 1], true);
		j++;
	}

	/* Uninstall error context callback. */
	error_context_stack = errcallback.previous;

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/*
	 * If we have a CTID to return, install it in both t_self and t_ctid.
	 * t_self is the normal place, but if the tuple is converted to a
	 * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
	 * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
	 */
	if (ctid)
		tuple->t_self = tuple->t_data->t_ctid = *ctid;

	/*
	 * Stomp on the xmin, xmax, and cmin fields from the tuple created by
	 * heap_form_tuple.  heap_form_tuple actually creates the tuple with
	 * DatumTupleFields, not HeapTupleFields, but the executor expects
	 * HeapTupleFields and will happily extract system columns on that
	 * assumption.  If we don't do this then, for example, the tuple length
	 * ends up in the xmin field, which isn't what we want.
	 */
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);

	/* Clean up */
	MemoryContextReset(temp_context);

	return tuple;
}


#if 0
static void get_retrieved_partial_aggfnoids(ForeignScanState *fsstate,
											List *retrieved_attrs,
											List **retrieved_aggfnoids)
{
	ListCell *lc = NULL;
	if (!fsstate)
	{
		foreach(lc, retrieved_attrs)
			*retrieved_aggfnoids = lappend_oid(*retrieved_aggfnoids, InvalidOid);
	}
	else
	{
		/* ForeignScan case */
		ForeignScan *fsplan = castNode(ForeignScan, fsstate->ss.ps.plan);
		if(!fsplan->fdw_scan_tlist)
		{
			foreach(lc, retrieved_attrs)
				*retrieved_aggfnoids = lappend_oid(*retrieved_aggfnoids, InvalidOid);
		}
		else
		{
			foreach(lc, fsplan->fdw_scan_tlist)
			{
				TargetEntry *tle = lfirst_node(TargetEntry, lc);

				if (tle->expr != NULL && nodeTag(tle->expr) == T_Aggref &&
					((Aggref *) tle->expr)->aggsplit == AGGSPLIT_INITIAL_SERIAL)
					*retrieved_aggfnoids = lappend_oid(*retrieved_aggfnoids, ((Aggref *) tle->expr)->aggfnoid);
				else
					*retrieved_aggfnoids = lappend_oid(*retrieved_aggfnoids, InvalidOid);
			}
		}
	}
}
#endif

/*
 * Callback function which is called when error occurs during column value
 * conversion.  Print names of column and relation.
 *
 * Note that this function mustn't do any catalog lookups, since we are in
 * an already-failed transaction.  Fortunately, we can get the needed info
 * from the relation or the query's rangetable instead.
 */
static void
conversion_error_callback(void *arg)
{
	ConversionLocation *errpos = (ConversionLocation *) arg;
	Relation	rel = errpos->rel;
	ForeignScanState *fsstate = errpos->fsstate;
	const char *attname = NULL;
	const char *relname = NULL;
	bool		is_wholerow = false;

	/*
	 * If we're in a scan node, always use aliases from the rangetable, for
	 * consistency between the simple-relation and remote-join cases.  Look at
	 * the relation's tupdesc only if we're not in a scan node.
	 */
	if (fsstate)
	{
		/* ForeignScan case */
		ForeignScan *fsplan = castNode(ForeignScan, fsstate->ss.ps.plan);
		int			varno = 0;
		AttrNumber	colno = 0;

		if (fsplan->scan.scanrelid > 0)
		{
			/* error occurred in a scan against a foreign table */
			varno = fsplan->scan.scanrelid;
			colno = errpos->cur_attno;
		}
		else
		{
			/* error occurred in a scan against a foreign join */
			TargetEntry *tle;

			tle = list_nth_node(TargetEntry, fsplan->fdw_scan_tlist,
								errpos->cur_attno - 1);

			/*
			 * Target list can have Vars and expressions.  For Vars, we can
			 * get some information, however for expressions we can't.  Thus
			 * for expressions, just show generic context message.
			 */
			if (IsA(tle->expr, Var))
			{
				Var		   *var = (Var *) tle->expr;

				varno = var->varno;
				colno = var->varattno;
			}
		}

		if (varno > 0)
		{
			EState	   *estate = fsstate->ss.ps.state;
			RangeTblEntry *rte = exec_rt_fetch(varno, estate);

			relname = rte->eref->aliasname;

			if (colno == 0)
				is_wholerow = true;
			else if (colno > 0 && colno <= list_length(rte->eref->colnames))
				attname = strVal(list_nth(rte->eref->colnames, colno - 1));
			else if (colno == SelfItemPointerAttributeNumber)
				attname = "ctid";
		}
	}
	else if (rel)
	{
		/* Non-ForeignScan case (we should always have a rel here) */
		TupleDesc	tupdesc = RelationGetDescr(rel);

		relname = RelationGetRelationName(rel);
		if (errpos->cur_attno > 0 && errpos->cur_attno <= tupdesc->natts)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc,
												   errpos->cur_attno - 1);

			attname = NameStr(attr->attname);
		}
		else if (errpos->cur_attno == SelfItemPointerAttributeNumber)
			attname = "ctid";
	}

	if (relname && is_wholerow)
		errcontext("whole-row reference to foreign table \"%s\"", relname);
	else if (relname && attname)
		errcontext("column \"%s\" of foreign table \"%s\"", attname, relname);
	else
		errcontext("processing expression at position %d in select list",
				   errpos->cur_attno);
}

/*
 * Given an EquivalenceClass and a foreign relation, find an EC member
 * that can be used to sort the relation remotely according to a pathkey
 * using this EC.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
find_em_for_rel(PlannerInfo *root, EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc;

	foreach(lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);

		/*
		 * Note we require !bms_is_empty, else we'd accept constant
		 * expressions which are not suitable for the purpose.
		 */
		if (bms_is_subset(em->em_relids, rel->relids) &&
			!bms_is_empty(em->em_relids) &&
			is_foreign_expr(root, rel, em->em_expr))
			return em;
	}

	return NULL;
}

/*
 * Find an EquivalenceClass member that is to be computed as a sort column
 * in the given rel's reltarget, and is shippable.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
find_em_for_rel_target(PlannerInfo *root, EquivalenceClass *ec,
					   RelOptInfo *rel)
{
	PathTarget *target = rel->reltarget;
	ListCell   *lc1;
	int			i;

	i = 0;
	foreach(lc1, target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc1);
		Index		sgref = get_pathtarget_sortgroupref(target, i);
		ListCell   *lc2;

		/* Ignore non-sort expressions */
		if (sgref == 0 ||
			get_sortgroupref_clause_noerr(sgref,
										  root->parse->sortClause) == NULL)
		{
			i++;
			continue;
		}

		/* We ignore binary-compatible relabeling on both ends */
		while (expr && IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;

		/* Locate an EquivalenceClass member matching this expr, if any */
		foreach(lc2, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc2);
			Expr	   *em_expr;

			/* Don't match constants */
			if (em->em_is_const)
				continue;

			/* Ignore child members */
			if (em->em_is_child)
				continue;

			/* Match if same expression (after stripping relabel) */
			em_expr = em->em_expr;
			while (em_expr && IsA(em_expr, RelabelType))
				em_expr = ((RelabelType *) em_expr)->arg;

			if (!equal(em_expr, expr))
				continue;

			/* Check that expression (including relabels!) is shippable */
			if (is_foreign_expr(root, rel, em->em_expr))
				return em;
		}

		i++;
	}

	return NULL;
}


static bool kite_get_relation_stats(PgFdwRelationInfo *fpinfo, Relation relation, kite_filespec_t *fspec,
		BlockNumber *totalpages, double *totalrows) {
	char errmsg[1024];
	ForeignTable *table;
	UserMapping *user;
	kite_request_t *req;
	StringInfoData sql, schema;
	List *retrieved_attrs, *aggfnoids;
	Datum datum;
	bool flag;
	xrg_agg_t *agg = 0;
	int64_t nrow = 0;

	retrieved_attrs = aggfnoids = NULL;

	/*
	 * Get the connection to use.  We do the remote access as the table's
	 * owner, even if the ANALYZE was started by some other user.
	 */
	table = GetForeignTable(RelationGetRelid(relation));
	user = GetUserMapping(relation->rd_rel->relowner, table->serverid);
	req = GetConnection(user, false);

	/*
	 * Construct command to get page count for relation.
	 */
	initStringInfo(&sql);
	deparseAnalyzeSizeSql(&sql, relation, &retrieved_attrs, &aggfnoids);

	//elog(LOG, "sql %s", sql.data);
	/*
         * build the schema for KITE
	 */
	{
		TupleDesc tupdesc = RelationGetDescr(relation);
		initStringInfo(&schema);
		kite_build_schema(&schema, tupdesc);
	}
	
	if (fpinfo->fragcnt <= 0) {
		fpinfo->fragcnt = getgpsegmentCount();
	}
	//elog(LOG, "scheam %s", schema.data);
	//elog(LOG, "fragcnt = %d", fpinfo->fragcnt);

	req->hdl = kite_submit(req->host, schema.data, sql.data, -1, fpinfo->fragcnt, fspec, errmsg, sizeof(errmsg));
	if (!req->hdl) {
		elog(ERROR, "kite_submit failed -- %s", errmsg);
		return false;
	}

	PG_TRY();
	{
		agg = xrg_agg_init(retrieved_attrs, aggfnoids, 0);

		xrg_agg_fetch(agg, req->hdl);

		if (xrg_agg_get_next(agg, 0, &datum, &flag, 1) != 0) {
			elog(ERROR, "kite: select count(*) return no result");
			return false;
		}
		nrow = DatumGetInt64(datum);
		*totalrows = (double)nrow;

		*totalpages = nrow / 200;
		if (*totalpages == 0) {
			*totalpages = 1;
		}

		elog(LOG, "Analyze totalrows = %ld, totalpages = %u", nrow, *totalpages);
	}
	PG_CATCH();
	{
		if (agg) xrg_agg_destroy(agg);
		ReleaseConnection(req);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (agg) xrg_agg_destroy(agg);
	ReleaseConnection(req);
	return true;
}

#if 0
static int
greenplumCheckIsGreenplum(UserMapping *user)
{
	PGconn     *conn;
	PGresult   *res;
	int                     ret;

	char *query =  "SELECT version()";

	conn = GetConnection(user, false);

	res = pgfdw_exec_query(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pgfdw_report_error(ERROR, res, conn, true, query);

	if (PQntuples(res) == 0)
		pgfdw_report_error(ERROR, res, conn, true, query);

	ret = strstr(PQgetvalue(res, 0, 0), "Greenplum Database") ? 1 : 0;

	PQclear(res);
	ReleaseConnection(conn);

	return ret;
}
#endif
