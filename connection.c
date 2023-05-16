/*-------------------------------------------------------------------------
 *
 * connection.c
 *		  Connection management functions for kite_fdw
 *
 * Portions Copyright (c) 2012-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/kite_fdw/connection.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "kite_fdw.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/* KITE */
static kite_request_t *connect_kite_server(ForeignServer *server, UserMapping *user) 
{
	kite_request_t *volatile req = NULL;

	PG_TRY();
	{
		const char **keywords;
		const char **values;
		int			n;
		char *host = NULL, *p;
		int fragcnt = 1;

		/*
		 * Construct connection params from generic options of ForeignServer
		 * and UserMapping.  (Some of them might not be libpq options, in
		 * which case we'll just waste a few array slots.)  Add 4 extra slots
		 * for application_name, fallback_application_name, client_encoding,
		 * end marker.
		 */
		n = list_length(server->options) + list_length(user->options) + 4;
		keywords = (const char **) palloc(n * sizeof(char *));
		values = (const char **) palloc(n * sizeof(char *));

		n = 0;
		n += ExtractConnectionOptions(server->options,
									  keywords + n, values + n);
		n += ExtractConnectionOptions(user->options,
									  keywords + n, values + n);

		/* search for host */
		for (int i = n - 1; i >= 0 ; i--) 
		{
			if (strcmp(keywords[i], "host") == 0 && 
					*(values[i]) != '\0')
			{
				host = pstrdup(values[i]);
				break;
			} 
		}
		p = host;
		while ((p = strchr(p, ',')) != NULL) {
			*p = '\n';
			p++;
		}

		req = (kite_request_t *) palloc0(sizeof(kite_request_t));
		req->host = host;
		req->hdl = 0;

		pfree(keywords);
		pfree(values);
	}
	PG_CATCH();
	{
		if (req) {
			if (req->host) pfree(req->host);
			pfree(req);
			req = 0;
		}
		PG_RE_THROW();

	}
	PG_END_TRY();

	return req;
}

kite_request_t *
GetConnection(UserMapping *user, bool will_prep_stmt)
{
        ForeignServer *server = GetForeignServer(user->serverid);
	return connect_kite_server(server, user);
}

/*
 * Release connection reference count created by calling GetConnection.
 */

/* KITE */
void
ReleaseConnection(kite_request_t *req)
{
	/*
	 * Currently, we don't actually track connection references because all
	 * cleanup is managed on a transaction or subtransaction basis instead. So
	 * there's nothing to do here.
	 */
	if (req) {
		if (req->host) pfree(req->host);
		if (req->hdl) kite_release(req->hdl);
		pfree(req);
	}

}
