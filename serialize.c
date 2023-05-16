#include <stdio.h>
#include <string.h>
#include "postgres.h"
#include "lib/stringinfo.h"
#include "kitesdk.h"
#include "kite_fdw.h"


/*
typedef struct kite_handle_t kite_handle_t;

typedef struct kite_filespec_t {
  char fmt[MAX_FILESPEC_FMT_LEN];

  union {
    struct {
      char delim;
      char quote;
      char escape;
      int8_t header_line;
      char nullstr[MAX_CSV_NULLSTR_LEN];
    } csv;
  } u;
} kite_filespec_t;
*/

extern void filespec_serialize(kite_filespec_t *fspec, StringInfo s) {

	appendStringInfo(s, "%s\n", fspec->fmt);
	appendStringInfo(s, "%c", fspec->u.csv.delim);
	appendStringInfo(s, "%c", fspec->u.csv.quote);
	appendStringInfo(s, "%c", fspec->u.csv.escape);
	appendStringInfo(s, "%c", (fspec->u.csv.header_line ? 'T' : 'F'));
	appendStringInfo(s, "%s", fspec->u.csv.nullstr);
}

extern kite_filespec_t *filespec_deserialize(char *src) {

	kite_filespec_t *fspec = (kite_filespec_t *) palloc0(sizeof(kite_filespec_t));
	char *s;
	char *p = pstrdup(src);
	int len = 0;

	s = strchr(p, '\n');
	if (!s) {
		return 0;
	}

	len = s-p;
	strncpy(fspec->fmt, p, s-p);
	fspec->fmt[len] = 0;
	s++;
	fspec->u.csv.delim = *s;
	s++;
	fspec->u.csv.quote = *s;
	s++;
	fspec->u.csv.escape = *s;
	s++;
	fspec->u.csv.header_line = (*s == 'T' ? 1 : 0);
	s++;
	strcpy(fspec->u.csv.nullstr, s);

	return fspec;
}
